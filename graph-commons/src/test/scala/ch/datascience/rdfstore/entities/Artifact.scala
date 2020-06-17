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

import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.rdfstore.FusekiBaseUrl
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed abstract class Artifact(val commitId:                  CommitId,
                               val filePath:                  FilePath,
                               val project:                   Project,
                               val maybeInvalidationActivity: Option[Activity])

object Artifact {

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Artifact] =
    new PartialEntityConverter[Artifact] {
      override def convert[T <: Artifact]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            Some(EntityId of fusekiBaseUrl / "blob" / entity.commitId / entity.filePath),
            EntityTypes of wfprov / "Artifact",
            rdfs / "label"            -> s"${entity.filePath}@${entity.commitId}".asJsonLD,
            schema / "isPartOf"       -> entity.project.asJsonLD,
            prov / "atLocation"       -> entity.filePath.asJsonLD,
            prov / "wasInvalidatedBy" -> entity.maybeInvalidationActivity.asJsonLD
          ).asRight
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Artifact] =
    JsonLDEncoder.instance {
      case e: Entity           => e.asJsonLD
      case e: EntityCollection => e.asJsonLD
      case e => throw new Exception(s"No JsonLD encoder found for ${e.getClass}")
    }
}

trait Entity {
  self: Artifact =>

  def maybeGeneration: Option[Generation]
}

object Entity {

  def apply(filePath: FilePath, generation: Generation): Artifact with Entity =
    new Artifact(generation.activity.id, filePath, generation.activity.project, maybeInvalidationActivity = None)
    with Entity {
      override val maybeGeneration: Option[Generation] = Option(generation)
    }

  def apply(commitId: CommitId, filePath: FilePath, project: Project): Artifact with Entity =
    new Artifact(commitId, filePath, project, maybeInvalidationActivity = None) with Entity {
      override val maybeGeneration: Option[Generation] = None
    }

  def apply(generation: Generation): Artifact with Entity =
    new Artifact(generation.activity.id,
                 generation.filePath,
                 generation.activity.project,
                 maybeInvalidationActivity = None) with Entity {
      override val maybeGeneration: Option[Generation] = Option(generation)
    }

  private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                 fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Entity] =
    new PartialEntityConverter[Entity] {
      override def convert[T <: Entity]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            maybeId = None,
            EntityTypes of prov / "Entity",
            prov / "qualifiedGeneration" -> entity.maybeGeneration.asJsonLD
          ).asRight
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[Artifact with Entity] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Artifact] combine entity.asPartialJsonLD[Entity] getOrFail
    }
}

trait EntityCollection {
  self: Artifact =>

  def collectionMembers: List[Artifact with Entity]
}

object EntityCollection {

  def apply(commitId: CommitId,
            filePath: FilePath,
            project:  Project,
            members:  List[Artifact with Entity]): Artifact with EntityCollection =
    new Artifact(commitId, filePath, project, maybeInvalidationActivity = None) with EntityCollection {
      override val collectionMembers: List[Artifact with Entity] = members
    }

  private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                 fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[EntityCollection] =
    new PartialEntityConverter[EntityCollection] {
      override def convert[T <: EntityCollection]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            maybeId = None,
            EntityTypes of (prov / "Entity", prov / "Collection"),
            prov / "hadMember" -> entity.collectionMembers.asJsonLD(JsonLDEncoder.encodeList(Entity.encoder))
          ).asRight
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[Artifact with EntityCollection] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Artifact] combine entity.asPartialJsonLD[EntityCollection] getOrFail
    }
}
