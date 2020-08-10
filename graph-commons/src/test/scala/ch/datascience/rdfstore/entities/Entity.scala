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
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.Collection.EntityCollection
import ch.datascience.rdfstore.entities.DataSet.DataSetEntity
import io.renku.jsonld._
import io.renku.jsonld.syntax._

import scala.language.postfixOps

class Entity(val commitId:                  CommitId,
             val location:                  Location,
             val project:                   Project,
             val maybeInvalidationActivity: Option[Activity],
             val maybeGeneration:           Option[Generation])

object Entity {

  def apply(generation: Generation): Entity with Artifact =
    new Entity(generation.activity.commitId,
               generation.location,
               generation.activity.project,
               maybeInvalidationActivity = None,
               maybeGeneration           = Some(generation)) with Artifact

  def factory(location: Location)(activity: Activity): Entity with Artifact =
    new Entity(activity.commitId, location, activity.project, maybeInvalidationActivity = None, maybeGeneration = None)
    with Artifact

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Entity] =
    new PartialEntityConverter[Entity] {
      override def convert[T <: Entity]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of prov / "Entity",
            rdfs / "label"               -> s"${entity.location}@${entity.commitId}".asJsonLD,
            schema / "isPartOf"          -> entity.project.asJsonLD,
            prov / "atLocation"          -> entity.location.asJsonLD,
            prov / "wasInvalidatedBy"    -> entity.maybeInvalidationActivity.asJsonLD,
            prov / "qualifiedGeneration" -> entity.maybeGeneration.asJsonLD
          ).asRight

      override def toEntityId: Entity => Option[EntityId] =
        entity => (EntityId of fusekiBaseUrl / "blob" / entity.commitId / entity.location).some
    }

  implicit def encoderWithArtifact(implicit renkuBaseUrl: RenkuBaseUrl,
                                   fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[Entity with Artifact] =
    JsonLDEncoder.instance {
      case e: EntityCollection     => e.asJsonLD
      case e: DataSetEntity        => e.asJsonLD
      case e: Entity with Artifact => e.asPartialJsonLD[Entity] combine e.asPartialJsonLD[Artifact] getOrFail
    }
}

trait Collection {
  self: Entity =>

  def collectionMembers: List[Entity with Artifact]
}

object Collection {

  type EntityCollection = Entity with Collection with Artifact

  def factory(location: Location, membersLocations: List[Location])(activity: Activity): EntityCollection =
    new Entity(
      commitId                  = activity.commitId,
      location                  = location,
      project                   = activity.project,
      maybeInvalidationActivity = None,
      maybeGeneration           = None
    ) with Collection with Artifact {
      override val collectionMembers: List[Entity with Artifact] = membersLocations.map { memberLocation =>
        new Entity(activity.commitId,
                   memberLocation,
                   activity.project,
                   maybeInvalidationActivity = None,
                   maybeGeneration           = None) with Artifact
      }
    }

  private implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                 fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[EntityCollection] =
    new PartialEntityConverter[EntityCollection] {
      override def convert[T <: EntityCollection]: T => Either[Exception, PartialEntity] =
        entity =>
          PartialEntity(
            EntityTypes of prov / "Collection",
            prov / "hadMember" -> entity.collectionMembers.asJsonLD
          ).asRight

      override def toEntityId: EntityCollection => Option[EntityId] =
        entity => (EntityId of fusekiBaseUrl / "blob" / entity.commitId / entity.location).some
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[EntityCollection] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Entity]
        .combine(entity.asPartialJsonLD[EntityCollection])
        .combine(entity.asPartialJsonLD[Artifact])
        .getOrFail
    }
}
