/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.entities

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.entities.Person
import io.renku.graph.model.{GraphClass, projects}
import io.renku.tinytypes._
import io.renku.triplesstore.client.sparql.LuceneQuery
import io.renku.triplesstore.client.syntax._
import search.Criteria.Filters

import java.time.{Instant, ZoneOffset}

package object search {

  private[search] implicit class CriteriaOps(criteria: Criteria) {

    def maybeOnAccessRightsAndVisibility(projectIdVariable: String, visibilityVariable: String): String =
      criteria.maybeUser match {
        case Some(user) =>
          val queriedVisibilities = criteria.filters.visibilities
          val nonPrivateVisibilities =
            if (queriedVisibilities.isEmpty)
              projects.Visibility.all - projects.Visibility.Private
            else (projects.Visibility.all - projects.Visibility.Private) intersect queriedVisibilities

          val selectPrivateValue =
            if (queriedVisibilities.isEmpty || queriedVisibilities.contains(projects.Visibility.Private))
              projects.Visibility.Private.asObject.asSparql.sparql
            else "''"
          s"""|{
              |  $projectIdVariable renku:projectVisibility $visibilityVariable .
              |  {
              |    VALUES ($visibilityVariable) {
              |      ${nonPrivateVisibilities.map(v => s"(${v.asObject.asSparql.sparql})").mkString(" ")}
              |    }
              |  } UNION {
              |    VALUES ($visibilityVariable) {
              |      ($selectPrivateValue)
              |    }
              |    $projectIdVariable schema:member ?memberId.
              |    GRAPH ${GraphClass.Persons.id.asSparql.sparql} {
              |      ?memberId schema:sameAs ?memberSameAs.
              |      ?memberSameAs schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject.asSparql.sparql};
              |                    schema:identifier ${user.id.asObject.asSparql.sparql}
              |    }
              |  }
              |}
              |""".stripMargin
        case _ =>
          criteria.filters.visibilities match {
            case v if v.isEmpty || v.contains(projects.Visibility.Public) =>
              s"""|$projectIdVariable renku:projectVisibility $visibilityVariable .
                  |VALUES ($visibilityVariable) { (${projects.Visibility.Public.asObject.asSparql.sparql}) }""".stripMargin
            case _ =>
              s"""|$projectIdVariable renku:projectVisibility $visibilityVariable .
                  |VALUES ($visibilityVariable) { ('') }""".stripMargin
          }
      }
  }

  private[search] implicit class FiltersOps(filters: Filters) {

    import io.renku.graph.model.views.SparqlLiteralEncoder.sparqlEncode

    lazy val query: LuceneQuery =
      filters.maybeQuery.map(q => LuceneQuery.escape(q.value)).getOrElse(LuceneQuery.queryAll)

    def whenRequesting(entityType: Filters.EntityType, predicates: Boolean*)(query: => String): Option[String] = {
      val typeMatching = filters.entityTypes match {
        case t if t.isEmpty => true
        case t              => t contains entityType
      }
      Option.when(typeMatching && predicates.forall(_ == true))(query)
    }

    def onQuery(snippet: String, matchingScoreVariableName: String = "?matchingScore"): String =
      foldQuery(_ => snippet, s"BIND (xsd:float(1.0) AS $matchingScoreVariableName)")

    def foldQuery[A](ifPresent: String => A, ifMissing: => A): A =
      if (query.isQueryAll) ifMissing
      else ifPresent(query.query)

    lazy val withNoOrPublicVisibility: Boolean = filters.visibilities match {
      case v if v.isEmpty => true
      case v              => v contains projects.Visibility.Public
    }

    def maybeOnCreatorName(variableName: String): String =
      filters.creators match {
        case creators if creators.isEmpty => ""
        case creators =>
          s"FILTER (IF (BOUND($variableName), LCASE($variableName) IN ${creators.map(_.toLowerCase.asSparqlEncodedLiteral).mkString("(", ", ", ")")}, false))"
      }

    def maybeOnCreatorsNames(variableName: String): String =
      filters.creators match {
        case creators if creators.isEmpty => ""
        case creators =>
          s"""FILTER (IF (BOUND($variableName), ${creators
              .map(c => s"CONTAINS (LCASE($variableName), ${c.toLowerCase.asSparqlEncodedLiteral})")
              .mkString(" || ")} , false))"""
      }

    def maybeOnNamespace(variableName: String): String =
      filters.namespaces match {
        case set if set.isEmpty => ""
        case set => s"VALUES ($variableName) { ${set.map(v => s"(${v.asObject.asSparql.sparql})").mkString(" ")} }"
      }

    def maybeOnDateCreated(variableName: String): String =
      List(
        filters.maybeSince map { since =>
          s"|BIND (${since.encodeAsXsdZonedDate} AS ?sinceZoned)" -> s"xsd:date($variableName) >= ?sinceZoned"
        },
        filters.maybeUntil map { until =>
          s"|BIND (${until.encodeAsXsdZonedDate} AS ?untilZoned)" -> s"xsd:date($variableName) <= ?untilZoned"
        }
      ).flatten.foldLeft(List.empty[String] -> List.empty[String]) { case ((binds, conditions), (bind, condition)) =>
        (bind :: binds) -> (condition :: conditions)
      } match {
        case (Nil, Nil) => ""
        case (binds, conditions) =>
          s"""${binds.mkString("\n")}
             |FILTER (${conditions.mkString(" && ")})""".stripMargin
      }

    def maybeOnDatasetDates(dateCreatedVariable: String, datePublishedVariable: String): String =
      List(
        filters.maybeSince map { since =>
          (
            s"""|BIND (${since.encodeAsXsdZonedDate} AS ?sinceZoned)
                |BIND (${since.encodeAsXsdNotZonedDate} AS ?sinceNotZoned)""".stripMargin,
            s"xsd:date($dateCreatedVariable) >= ?sinceZoned",
            s"xsd:date($datePublishedVariable) >= ?sinceNotZoned"
          )
        },
        filters.maybeUntil map { until =>
          (
            s"""|BIND (${until.encodeAsXsdZonedDate} AS ?untilZoned)
                |BIND (${until.encodeAsXsdNotZonedDate} AS ?untilNotZoned)""".stripMargin,
            s"xsd:date($dateCreatedVariable) <= ?untilZoned",
            s"xsd:date($datePublishedVariable) <= ?untilNotZoned"
          )
        }
      ).flatten.foldLeft(List.empty[String], List.empty[String], List.empty[String]) {
        case ((binds, zonedConditions, notZonedConditions), (bind, zonedCondition, notZonedCondition)) =>
          (bind :: binds, zonedCondition :: zonedConditions, notZonedCondition :: notZonedConditions)
      } match {
        case (Nil, Nil, Nil) => ""
        case (binds, zonedConditions, notZonedConditions) =>
          s"""${binds.mkString("\n")}
             |FILTER (
             |  IF (
             |    BOUND($dateCreatedVariable),
             |      ${zonedConditions.mkString(" && ")},
             |      (IF (
             |        BOUND($datePublishedVariable),
             |          ${notZonedConditions.mkString(" && ")},
             |          false
             |      ))
             |  )
             |)""".stripMargin
      }

    private implicit class DateOps(date: LocalDateTinyType) {

      lazy val encodeAsXsdZonedDate: String =
        s"xsd:date(xsd:dateTime('${Instant.from(date.value.atStartOfDay(ZoneOffset.UTC))}'))"

      lazy val encodeAsXsdNotZonedDate: String = s"xsd:date('$date')"
    }

    private implicit class ValueOps[TT <: TinyType](v: TT)(implicit s: Show[TT]) {
      lazy val asSparqlEncodedLiteral: String = s"'${sparqlEncode(v.show)}'"
    }

    private implicit class StringValueOps[TT <: StringTinyType](v: TT)(implicit s: Show[TT]) {
      def toLowerCase(implicit factory: TinyTypeFactory[TT]): TT = factory(v.show.toLowerCase())
    }
  }
}
