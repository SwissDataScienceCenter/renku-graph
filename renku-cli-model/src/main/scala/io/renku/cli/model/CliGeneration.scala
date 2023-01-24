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

package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliGeneration.GenerationEntity
import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.generations._
import io.renku.graph.model.{activities, entityModel}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliGeneration(
    resourceId:         ResourceId,
    entity:             GenerationEntity,
    activityResourceId: activities.ResourceId
) extends CliModel

object CliGeneration {

  sealed trait GenerationEntity {
    def resourceId:    entityModel.ResourceId
    def generationIds: List[ResourceId]
    def fold[A](fa: CliEntity => A, fb: CliCollectionEntity => A): A
  }

  object GenerationEntity {
    final case class Entity(entity: CliEntity) extends GenerationEntity {
      val resourceId:    entityModel.ResourceId = entity.resourceId
      val generationIds: List[ResourceId]       = entity.generationIds
      def fold[A](fa: CliEntity => A, fb: CliCollectionEntity => A): A = fa(entity)
    }

    final case class Collection(collection: CliCollectionEntity) extends GenerationEntity {
      val resourceId:    entityModel.ResourceId = collection.resourceId
      val generationIds: List[ResourceId]       = collection.generationIds
      def fold[A](fa: CliEntity => A, fb: CliCollectionEntity => A): A = fb(collection)
    }

    def apply(entity: CliEntity):           GenerationEntity = Entity(entity)
    def apply(coll:   CliCollectionEntity): GenerationEntity = Collection(coll)

    private val entityTypes = EntityTypes.of(Prov.Entity)

    private def selectCandidates(ets: EntityTypes): Boolean =
      CliEntity.matchingEntityTypes(ets) || CliCollectionEntity.matchingEntityTypes(ets)

    implicit def jsonLDDecoder: JsonLDDecoder[GenerationEntity] = {
      val da = CliEntity.jsonLDDecoder.emap(e => Right(GenerationEntity(e)))
      val db = CliCollectionEntity.jsonLdDecoder.emap(e => Right(GenerationEntity(e)))
      JsonLDDecoder.entity(entityTypes, _.getEntityTypes.map(selectCandidates)) { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliEntity.matchingEntityTypes), currentTypes.map(CliCollectionEntity.matchingEntityTypes))
          .flatMapN {
            case (_, true) => db(cursor)
            case (true, _) => da(cursor)
            case _ =>
              Left(
                DecodingFailure(s"Invalid entity types for decoding related entity for a generation: $entityTypes", Nil)
              )
          }
      }
    }

    implicit def jsonLDEncoder: JsonLDEncoder[GenerationEntity] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Generation)

  implicit lazy val jsonLDEncoder: JsonLDEncoder[CliGeneration] =
    JsonLDEncoder.instance { generation =>
      JsonLD.entity(
        generation.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe(Prov.qualifiedGeneration -> generation.entity.asJsonLD),
        Prov.activity -> generation.activityResourceId.asEntityId.asJsonLD
      )
    }

  implicit val jsonLDDecoder: JsonLDDecoder[CliGeneration] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId  <- cursor.downEntityId.as[ResourceId]
        activityId  <- cursor.downField(Prov.activity).downEntityId.as[activities.ResourceId]
        allEntities <- cursor.focusTop.as[List[GenerationEntity]]
        entity <- allEntities
                    .find(_.generationIds.contains(resourceId))
                    .toRight(DecodingFailure(s"No related entity found for generation '$resourceId'", Nil))
      } yield CliGeneration(resourceId, entity, activityId)
    }
}
