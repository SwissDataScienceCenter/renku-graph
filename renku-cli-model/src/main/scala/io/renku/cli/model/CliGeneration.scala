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
import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.activities
import io.renku.graph.model.generations._
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import monocle.Lens

final case class CliGeneration(
    resourceId:         ResourceId,
    entity:             CliEntity,
    activityResourceId: activities.ResourceId
) extends CliModel

object CliGeneration {

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
    jsonLDDecoderFor(_ => Right(true))

  def decoderForActivity(activityId: activities.ResourceId): JsonLDDecoder[CliGeneration] =
    jsonLDDecoderFor(withActivity(activityId))

  private def jsonLDDecoderFor(filter: Cursor => Result[Boolean]): JsonLDDecoder[CliGeneration] =
    JsonLDDecoder.entity(entityTypes, filter) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        activityId <- cursor.downField(Prov.activity).downEntityId.as[activities.ResourceId]
        entity <-
          cursor.focusTop
            .as[List[CliEntity]](JsonLDDecoder.decodeList(CliEntity.jsonLDDecoderForGeneration(resourceId)))
            .flatMap {
              case entity :: Nil => entity.asRight
              case _ =>
                DecodingFailure(s"Generation $resourceId without or with multiple entities ", Nil).asLeft
            }
      } yield CliGeneration(resourceId, entity, activityId)
    }

  private def withActivity(activityId: activities.ResourceId): Cursor => JsonLDDecoder.Result[Boolean] =
    _.downField(Prov.activity).downEntityId.as[activities.ResourceId].map(_ == activityId)

  object Lenses {
    val entity: Lens[CliGeneration, CliEntity] =
      Lens[CliGeneration, CliEntity](_.entity)(e => _.copy(entity = e))

    val entityPath: Lens[CliGeneration, EntityPath] =
      entity.composeLens(CliEntity.Lenses.entityPath)
  }
}
