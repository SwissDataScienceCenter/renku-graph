/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.entities

import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.usages.ResourceId
import io.renku.jsonld._
import io.renku.jsonld.syntax.JsonEncoderOps

final case class Usage(resourceId: ResourceId, entity: Entity)

object Usage {

  val entityTypes: EntityTypes = EntityTypes of (prov / "Usage")

  implicit lazy val encoder: JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { case Usage(resourceId, entity) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        prov / "entity" -> entity.asJsonLD
      )
    }

  implicit lazy val decoder: JsonLDDecoder[Usage] = JsonLDDecoder.entity(entityTypes) { cursor =>
    for {
      resourceId <- cursor.downEntityId.as[ResourceId]
      entity     <- cursor.downField(prov / "entity").as[Entity]
    } yield Usage(resourceId, entity)
  }
}
