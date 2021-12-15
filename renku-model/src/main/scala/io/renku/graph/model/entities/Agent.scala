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

package io.renku.graph.model.entities

import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.agents._
import io.renku.jsonld._

final case class Agent(resourceId: ResourceId, name: Name)

object Agent {

  val entityTypes: EntityTypes = EntityTypes.of(prov / "SoftwareAgent")

  implicit lazy val encoder: JsonLDEncoder[Agent] = JsonLDEncoder.instance { agent =>
    import io.renku.graph.model.Schemas._
    import io.renku.jsonld.syntax._

    JsonLD.entity(
      agent.resourceId.asEntityId,
      entityTypes,
      schema / "name" -> agent.name.asJsonLD
    )
  }

  implicit lazy val decoder: JsonLDDecoder[Agent] = JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
    for {
      resourceId <- cursor.downEntityId.as[ResourceId]
      label      <- cursor.downField(schema / "name").as[Name]
    } yield Agent(resourceId, label)
  }
}
