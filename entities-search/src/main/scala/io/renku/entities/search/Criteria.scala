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

package io.renku.entities.search

import Criteria._
import io.circe.Decoder
import io.renku.graph.model.{persons, projects}
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.server.security.model.AuthUser
import io.renku.tinytypes.constraints.{LocalDateNotInTheFuture, NonBlank}
import io.renku.tinytypes.json.TinyTypeDecoders
import io.renku.tinytypes.{BooleanTinyType, LocalDateTinyType, StringTinyType, TinyTypeFactory}

import java.time.LocalDate

final case class Criteria(filters:   Filters = Filters(),
                          sorting:   Sorting[Criteria.Sort.type] = Criteria.Sort.default,
                          paging:    PagingRequest = PagingRequest.default,
                          maybeUser: Option[AuthUser] = None
)

object Criteria {

  final case class Filters(maybeQuery:   Option[Filters.Query] = None,
                           entityTypes:  Set[Filters.EntityType] = Set.empty,
                           creators:     Set[persons.Name] = Set.empty,
                           maybeOwned:   Option[Filters.Owned] = None,
                           visibilities: Set[projects.Visibility] = Set.empty,
                           namespaces:   Set[projects.Namespace] = Set.empty,
                           maybeSince:   Option[Filters.Since] = None,
                           maybeUntil:   Option[Filters.Until] = None
  )

  object Filters {

    final class Query private (val value: String) extends AnyVal with StringTinyType
    object Query                                  extends TinyTypeFactory[Query](new Query(_)) with NonBlank[Query]

    final class Owned private (val value: Boolean) extends AnyVal with BooleanTinyType
    object Owned                                   extends TinyTypeFactory[Owned](new Owned(_))

    sealed trait EntityType extends StringTinyType with Product
    object EntityType extends TinyTypeFactory[EntityType](EntityTypeApply) {

      final case object Project  extends EntityType { override val value: String = "project" }
      final case object Dataset  extends EntityType { override val value: String = "dataset" }
      final case object Workflow extends EntityType { override val value: String = "workflow" }
      final case object Person   extends EntityType { override val value: String = "person" }

      val all: List[EntityType] = Project :: Dataset :: Workflow :: Person :: Nil

      implicit val decoder: Decoder[EntityType] = TinyTypeDecoders.stringDecoder(EntityType)
    }

    private object EntityTypeApply extends (String => EntityType) {
      override def apply(value: String): EntityType = EntityType.all.find(_.value == value).getOrElse {
        throw new IllegalArgumentException(s"'$value' unknown EntityType")
      }
    }

    final class Since private (val value: LocalDate) extends AnyVal with LocalDateTinyType
    object Since extends TinyTypeFactory[Since](new Since(_)) with LocalDateNotInTheFuture[Since]

    final class Until private (val value: LocalDate) extends AnyVal with LocalDateTinyType
    object Until extends TinyTypeFactory[Until](new Until(_)) with LocalDateNotInTheFuture[Until]
  }

  object Sort extends io.renku.http.rest.SortBy {

    type PropertyType = SortProperty

    sealed trait SortProperty extends Property

    final case object ByName          extends Property("name") with SortProperty
    final case object ByMatchingScore extends Property("matchingScore") with SortProperty
    final case object ByDate          extends Property("date") with SortProperty
    final case object ByDateModified  extends Property("dateModified") with SortProperty

    val byNameAsc: Sort.By = Sort.By(ByName, Direction.Asc)

    val default: Sorting[Sort.type] = Sorting(byNameAsc)

    override lazy val properties: Set[SortProperty] = Set(ByName, ByMatchingScore, ByDate, ByDateModified)
  }
}
