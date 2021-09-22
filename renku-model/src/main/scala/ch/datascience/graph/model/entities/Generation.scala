/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.graph.model.Schemas.prov
import ch.datascience.graph.model.entities.Entity.OutputEntity
import ch.datascience.graph.model.generations.ResourceId
import ch.datascience.graph.model.{activities, generations}
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder.decodeList
import io.renku.jsonld._
import io.renku.jsonld.syntax.JsonEncoderOps

final case class Generation(resourceId: ResourceId, activityResourceId: activities.ResourceId, entity: OutputEntity)

object Generation {

  private val entityTypes: EntityTypes = EntityTypes of prov / "Generation"

  implicit lazy val encoder: JsonLDEncoder[Generation] =
    JsonLDEncoder.instance { generation =>
      JsonLD.entity(
        generation.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe(prov / "qualifiedGeneration" -> generation.entity.asJsonLD),
        prov / "activity" -> generation.activityResourceId.asEntityId.asJsonLD
      )
    }

  implicit lazy val decoder: JsonLDDecoder[Generation] = JsonLDDecoder.entity(entityTypes) { cursor =>
    val multipleToNone: List[OutputEntity] => Either[DecodingFailure, Option[OutputEntity]] = {
      case entity :: Nil => Right(Some(entity))
      case _             => Right(None)
    }

    for {
      resourceId         <- cursor.downEntityId.as[generations.ResourceId]
      activityResourceId <- cursor.downField(prov / "activity").downEntityId.as[activities.ResourceId]
      entity <- cursor.top
                  .map {
                    _.cursor
                      .as[List[OutputEntity]](decodeList(Entity.outputEntityDecoder(resourceId)))
                      .flatMap(multipleToNone)
                  }
                  .flatMap(_.sequence)
                  .getOrElse(Left(DecodingFailure("Generation without or with multiple entities", Nil)))
    } yield Generation(resourceId, activityResourceId, entity)
  }

}
