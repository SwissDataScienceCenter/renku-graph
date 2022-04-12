/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.entities

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.knowledgegraph.entities.Endpoint.Criteria
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters
import io.renku.tinytypes.TinyType

import java.time.{Instant, ZoneOffset}

package object finder {

  private[finder] implicit class CriteriaOps(criteria: Criteria) {

    def maybeOnAccessRights(projectIdVariable: String, visibilityVariable: String): String = criteria.maybeUser match {
      case Some(user) =>
        s"""|OPTIONAL {
            |    $projectIdVariable schema:member/schema:sameAs ?memberId.
            |    ?memberId schema:additionalType 'GitLab';
            |              schema:identifier ?userGitlabId .
            |}
            |FILTER (
            |  $visibilityVariable != '${projects.Visibility.Private.value}' || ?userGitlabId = ${user.id.value}
            |)
            |""".stripMargin
      case _ =>
        s"""FILTER ($visibilityVariable = '${projects.Visibility.Public.value}')"""
    }
  }

  private[finder] implicit class FiltersOps(filters: Filters) {
    import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode

    lazy val query: String = filters.maybeQuery.map(_.value).getOrElse("*")

    def whenRequesting(entityType: Filters.EntityType, predicates: Boolean*)(query: => String): Option[String] = {
      val typeMatching = filters.entityTypes match {
        case t if t.isEmpty => true
        case t              => t contains entityType
      }
      Option.when(typeMatching && predicates.forall(_ == true))(query)
    }

    lazy val withNoOrPublicVisibility: Boolean = filters.visibilities match {
      case v if v.isEmpty => true
      case v              => v contains projects.Visibility.Public
    }

    def maybeOnCreatorName(variableName: String): String =
      filters.maybeCreator
        .map { creator =>
          s"FILTER (IF (BOUND($variableName), $variableName = '${sparqlEncode(creator.show)}', false))"
        }
        .getOrElse("")

    def maybeOnCreatorsNames(variableName: String): String =
      filters.maybeCreator
        .map { creator =>
          s"FILTER (IF (BOUND($variableName), CONTAINS($variableName, '${sparqlEncode(creator.show)}'), false))"
        }
        .getOrElse("")

    def maybeOnVisibility(variableName: String): String =
      filters.visibilities match {
        case set if set.isEmpty => ""
        case set                => s"FILTER ($variableName IN ${set.map(_.asLiteral).mkString("(", ", ", ")")})"
      }

    def maybeOnDateCreated(variableName: String): String =
      filters.maybeDate
        .map(date => s"""|BIND (${date.encodeAsXsdZonedDate} AS ?dateZoned)
                         |FILTER (xsd:date($variableName) = ?dateZoned)""".stripMargin)
        .getOrElse("")

    def maybeOnDatasetDates(dateCreatedVariable: String, datePublishedVariable: String): String =
      filters.maybeDate
        .map(date => s"""|BIND (${date.encodeAsXsdZonedDate} AS ?dateZoned)
                         |BIND (${date.encodeAsXsdNotZonedDate} AS ?dateNotZoned)
                         |FILTER (
                         |  IF (
                         |    BOUND($dateCreatedVariable), 
                         |      xsd:date($dateCreatedVariable) = ?dateZoned, 
                         |      (IF (
                         |        BOUND($datePublishedVariable), 
                         |          xsd:date($datePublishedVariable) = ?dateNotZoned, 
                         |          false
                         |      ))
                         |  )
                         |)""".stripMargin)
        .getOrElse("")

    private implicit class DateOps(date: Filters.Date) {

      lazy val encodeAsXsdZonedDate: String =
        s"xsd:date(xsd:dateTime('${Instant.from(date.value.atStartOfDay(ZoneOffset.UTC))}'))"

      lazy val encodeAsXsdNotZonedDate: String = s"xsd:date('$date')"
    }

    private implicit class ValueOps[TT <: TinyType](v: TT)(implicit show: Show[TT]) {
      lazy val asLiteral: String = show"'$v'"
    }
  }

  private[finder] object DecodingTools {
    import io.circe.{Decoder, DecodingFailure}
    import io.renku.tinytypes._

    def toListOf[TT <: StringTinyType, TTF <: TinyTypeFactory[TT]](implicit
        ttFactory: TTF
    ): Option[String] => Decoder.Result[List[TT]] =
      _.map(_.split(',').toList.map(v => ttFactory.from(v)).sequence.map(_.sortBy(_.value))).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(List.empty))

    def toListOfImageUris[TT <: TinyType { type V = String }, TTF <: From[TT]](implicit
        ttFactory: TTF
    ): Option[String] => Decoder.Result[List[TT]] =
      _.map(
        _.split(",")
          .map(_.trim)
          .map { case s"$position:$url" => ttFactory.from(url).map(tt => position.toIntOption.getOrElse(0) -> tt) }
          .toList
          .sequence
          .map(_.distinct.sortBy(_._1).map(_._2))
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
      ).getOrElse(Nil.asRight)
  }
}
