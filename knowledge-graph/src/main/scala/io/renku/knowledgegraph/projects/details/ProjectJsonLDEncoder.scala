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

package io.renku.knowledgegraph.projects.details

import io.renku.graph.model.Schemas._
import io.renku.graph.model.entities
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import model._

private trait ProjectJsonLDEncoder {
  def encode(project: model.Project): JsonLD
}

private object ProjectJsonLDEncoder extends ProjectJsonLDEncoder {

  override def encode(project: model.Project): JsonLD = project.asJsonLD

  private implicit lazy val encoder: JsonLDEncoder[Project] = JsonLDEncoder.instance { project =>
    JsonLD.entity(
      project.resourceId.asEntityId,
      entities.Project.entityTypes,
      schema / "identifier"       -> project.id.asJsonLD,
      renku / "projectPath"       -> project.slug.asJsonLD,
      schema / "name"             -> project.name.asJsonLD,
      schema / "description"      -> project.maybeDescription.asJsonLD,
      renku / "projectVisibility" -> project.visibility.asJsonLD,
      schema / "dateCreated"      -> project.created.date.asJsonLD,
      schema / "creator"          -> project.created.maybeCreator.asJsonLD,
      schema / "dateModified"     -> project.dateModified.asJsonLD,
      schema / "keywords"         -> project.keywords.asJsonLD,
      schema / "schemaVersion"    -> project.maybeVersion.asJsonLD,
      prov / "wasDerivedFrom"     -> project.forking.maybeParent.asJsonLD,
      schema / "image"            -> project.images.asJsonLD
    )
  }

  private implicit lazy val creatorEncoder: JsonLDEncoder[Creator] = JsonLDEncoder.instance { creator =>
    JsonLD.entity(
      creator.resourceId.asEntityId,
      entities.Person.entityTypes,
      schema / "name"        -> creator.name.asJsonLD,
      schema / "email"       -> creator.maybeEmail.asJsonLD,
      schema / "affiliation" -> creator.maybeAffiliation.asJsonLD
    )
  }

  private implicit lazy val parentDecoder: JsonLDEncoder[ParentProject] = JsonLDEncoder.instance { parent =>
    JsonLD.entity(
      parent.resourceId.asEntityId,
      entities.Project.entityTypes,
      renku / "projectPath"  -> parent.slug.asJsonLD,
      schema / "name"        -> parent.name.asJsonLD,
      schema / "dateCreated" -> parent.created.date.asJsonLD,
      schema / "creator"     -> parent.created.maybeCreator.asJsonLD
    )
  }
}
