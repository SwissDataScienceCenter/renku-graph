/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.entities

import cats.implicits.catsSyntaxOptionId
import ch.datascience.graph.model.projects.{DateCreated, Name, Path, ResourceId, SchemaVersion}
import ch.datascience.rdfstore.FusekiBaseUrl

final case class Project(path:               Path,
                         name:               Name,
                         dateCreated:        DateCreated,
                         maybeCreator:       Option[Person],
                         maybeParentProject: Option[Project] = None,
                         version:            SchemaVersion
)

object Project {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  private[entities] implicit def converter(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): PartialEntityConverter[Project] =
    new PartialEntityConverter[Project] {
      override def convert[T <: Project]: T => Either[Exception, PartialEntity] =
        entity =>
          Right(
            PartialEntity(
              EntityTypes.of(prov / "Location", schema / "Project"),
              schema / "name"          -> entity.name.asJsonLD,
              schema / "dateCreated"   -> entity.dateCreated.asJsonLD,
              schema / "creator"       -> entity.maybeCreator.asJsonLD,
              schema / "schemaVersion" -> entity.version.asJsonLD,
              prov / "wasDerivedFrom"  -> entity.maybeParentProject.asJsonLD
            )
          )

      override def toEntityId: Project => Option[EntityId] =
        entity => (EntityId of ResourceId(renkuBaseUrl, entity.path)).some
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Project] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Project].getOrFail
    }

  implicit def entityIdEncoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): EntityIdEncoder[Project] =
    EntityIdEncoder.instance { entity =>
      converter.toEntityId(entity).getOrElse(throw new IllegalStateException(s"No EntityId found for $entity"))

    }

  private implicit val projectResourceToEntityId: ResourceId => EntityId =
    resource => EntityId of resource.value

}
