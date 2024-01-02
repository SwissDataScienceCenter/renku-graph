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

package io.renku.entities.viewings.search

import cats.NonEmptyParallel
import cats.effect._
import io.circe.Decoder
import io.renku.entities.search.model.Entity
import io.renku.http.rest.paging.PagingResponse
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait RecentEntitiesFinder[F[_]] {

  def findRecentlyViewedEntities(criteria: RecentEntitiesFinder.Criteria): F[PagingResponse[Entity]]

}

object RecentEntitiesFinder {
  sealed trait EntityType extends Product {
    final val name: String = productPrefix.toLowerCase
  }
  object EntityType {
    case object Project extends EntityType
    case object Dataset extends EntityType

    val all: List[EntityType] = List(Project, Dataset)

    def fromString(str: String): Either[String, EntityType] =
      all.find(_.name.equalsIgnoreCase(str)).toRight(s"Invalid entity type: $str")

    implicit val jsonDecoder: Decoder[EntityType] =
      Decoder.decodeString.emap(fromString)
  }

  final case class Criteria(
      entityTypes: Set[EntityType],
      authUser:    AuthUser,
      limit:       Int
  ) {
    def forType(et: EntityType): Boolean =
      entityTypes.isEmpty || entityTypes.contains(et)
  }

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
      connConfig: ProjectsConnectionConfig
  ): F[RecentEntitiesFinder[F]] =
    Async[F].pure(new RecentEntitiesFinderImpl[F](connConfig))

}
