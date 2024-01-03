/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.entities.search.Criteria.Filters
import io.renku.graph.model.projects
import io.renku.tinytypes._
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import io.renku.triplesstore.client.syntax._
import io.renku.graph.model.views.TinyTypeToObject._

import java.time.{Instant, ZoneOffset}

package object search {

  private[search] implicit class FiltersOps(filters: Filters) {

    lazy val query: LuceneQuery =
      filters.maybeQuery.map(q => LuceneQuery.fuzzy(q.value)).getOrElse(LuceneQuery.queryAll)

    def whenRequesting(entityType: Filters.EntityType, predicates: Boolean*)(query: => Fragment): Option[Fragment] = {
      val typeMatching = filters.entityTypes match {
        case t if t.isEmpty => true
        case t              => t contains entityType
      }
      Option.when(typeMatching && predicates.forall(_ == true))(query)
    }

    def onQuery(snippet: Fragment, matchingScoreVariableName: VarName = VarName("matchingScore")): Fragment =
      foldQuery(_ => snippet, fr"BIND (xsd:float(1.0) AS $matchingScoreVariableName)")

    def foldQuery[A](ifPresent: LuceneQuery => A, ifMissing: => A): A =
      if (query.isQueryAll) ifMissing
      else ifPresent(query)

    lazy val withNoOrPublicVisibility: Boolean = filters.visibilities match {
      case v if v.isEmpty => true
      case v              => v contains projects.Visibility.Public
    }

    def maybeOnCreatorName(variableName: VarName): Fragment =
      filters.creators match {
        case creators if creators.isEmpty => Fragment.empty
        case creators =>
          fr"FILTER (IF (BOUND($variableName), LCASE($variableName) IN (${creators.map(_.toLowerCase).map(_.asObject)}), false))"
      }

    def maybeOnNamespace(variableName: VarName): Fragment =
      filters.namespaces match {
        case set if set.isEmpty => Fragment.empty
        case set                => fr"VALUES ($variableName) { ${set.map(_.asObject)} }"
      }

    def maybeOnDateCreated(variableName: VarName): Fragment =
      List(
        filters.maybeSince map { since =>
          fr"|BIND (${since.encodeAsXsdZonedDate} AS ?sinceZoned)" -> sparql"xsd:date($variableName) >= ?sinceZoned"
        },
        filters.maybeUntil map { until =>
          fr"|BIND (${until.encodeAsXsdZonedDate} AS ?untilZoned)" -> sparql"xsd:date($variableName) <= ?untilZoned"
        }
      ).flatten.foldLeft(List.empty[Fragment] -> List.empty[Fragment]) {
        case ((binds, conditions), (bind, condition)) =>
          (bind :: binds) -> (condition :: conditions)
      } match {
        case (Nil, Nil) => Fragment.empty
        case (binds, conditions) =>
          fr"""|${binds.intercalate(fr"\n")}
               |FILTER (${conditions.intercalate(fr" && ")})""".stripMargin
      }

    private implicit class DateOps(date: LocalDateTinyType) {
      lazy val encodeAsXsdZonedDate: Fragment =
        fr"xsd:date(xsd:dateTime(${Instant.from(date.value.atStartOfDay(ZoneOffset.UTC))}))"
    }

    private implicit class StringValueOps[TT <: StringTinyType](v: TT)(implicit s: Show[TT]) {
      def toLowerCase(implicit factory: TinyTypeFactory[TT]): TT = factory(v.show.toLowerCase())
    }
  }
}
