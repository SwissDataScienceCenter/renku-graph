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

import io.renku.graph.model.{persons, projects}
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.security.model.AuthUser
import io.renku.tinytypes.constraints.{LocalDateNotInTheFuture, NonBlank}
import io.renku.tinytypes.{LocalDateTinyType, StringTinyType, TinyTypeFactory}

import java.time.LocalDate

object Endpoint {

  import Criteria._

  final case class Criteria(filters:   Filters = Filters(),
                            sorting:   Sorting.By = Sorting.default,
                            paging:    PagingRequest = PagingRequest.default,
                            maybeUser: Option[AuthUser] = None
  )

  object Criteria {

    final case class Filters(maybeQuery:      Option[Filters.Query] = None,
                             maybeEntityType: Option[Filters.EntityType] = None,
                             maybeCreator:    Option[persons.Name] = None,
                             maybeVisibility: Option[projects.Visibility] = None,
                             maybeDate:       Option[Filters.Date] = None
    )

    object Filters {

      final class Query private (val value: String) extends AnyVal with StringTinyType
      object Query                                  extends TinyTypeFactory[Query](new Query(_)) with NonBlank

      sealed trait EntityType extends StringTinyType with Product with Serializable
      object EntityType extends TinyTypeFactory[EntityType](EntityTypeApply) {

        final case object Project extends EntityType { override val value: String = "project" }
        final case object Dataset extends EntityType { override val value: String = "dataset" }
        final case object Person  extends EntityType { override val value: String = "person" }

        val all: List[EntityType] = Project :: Dataset :: Person :: Nil
      }

      private object EntityTypeApply extends (String => EntityType) {
        override def apply(value: String): EntityType = EntityType.all.find(_.value == value).getOrElse {
          throw new IllegalArgumentException(s"'$value' unknown EntityType")
        }
      }

      final class Date private (val value: LocalDate) extends AnyVal with LocalDateTinyType
      object Date extends TinyTypeFactory[Date](new Date(_)) with LocalDateNotInTheFuture
    }

    object Sorting extends io.renku.http.rest.SortBy {

      type PropertyType = SortProperty

      sealed trait SortProperty extends Property

      final case object ByName          extends Property("name") with SortProperty
      final case object ByMatchingScore extends Property("matchingScore") with SortProperty
      final case object ByDate          extends Property("date") with SortProperty

      lazy val default: Sorting.By = Sorting.By(ByName, Direction.Asc)

      override lazy val properties: Set[SortProperty] = Set(ByName, ByMatchingScore, ByDate)
    }
  }
}
