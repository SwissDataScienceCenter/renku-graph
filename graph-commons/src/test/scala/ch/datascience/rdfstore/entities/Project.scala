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

import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.projects.{DateCreated, Name, Path, ResourceId}

final case class Project(path:               Path,
                         name:               Name,
                         dateCreated:        DateCreated,
                         creator:            Person,
                         maybeParentProject: Option[Project] = None,
                         version:            SchemaVersion = SchemaVersion("1"))

object Project {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Project] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId of ResourceId(renkuBaseUrl, entity.path),
      EntityTypes of (prov / "Location", schema / "Project"),
      schema / "name"          -> entity.name.asJsonLD,
      schema / "dateCreated"   -> entity.dateCreated.asJsonLD,
      schema / "dateUpdated"   -> entity.dateCreated.asJsonLD,
      schema / "creator"       -> entity.creator.asJsonLD,
      schema / "schemaVersion" -> entity.version.asJsonLD,
      prov / "wasDerivedFrom"  -> entity.maybeParentProject.asJsonLD
    )
  }

  private implicit val projectResourceToEntityId: ResourceId => EntityId =
    resource => EntityId of resource.value
}
