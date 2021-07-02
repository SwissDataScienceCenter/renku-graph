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

import ch.datascience.graph.model.Schemas.prov
import ch.datascience.graph.model.associations.ResourceId
import io.renku.jsonld.syntax.JsonEncoderOps
import io.renku.jsonld._

final case class Association(resourceId: ResourceId, agent: Agent, runPlan: RunPlan)

object Association {

  private val entityTypes = EntityTypes of (prov / "Association")

  implicit lazy val encoder: JsonLDEncoder[Association] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entity.resourceId.asEntityId,
        entityTypes,
        prov / "agent"   -> entity.agent.asJsonLD,
        prov / "hadPlan" -> entity.runPlan.resourceId.asEntityId.asJsonLD
      )
    }

  implicit lazy val decoder: JsonLDDecoder[Association] = JsonLDDecoder.entity(entityTypes) { cursor =>
    import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._
    for {
      resourceId <- cursor.downEntityId.as[ResourceId]
      agent      <- cursor.downField(prov / "agent").as[Agent]
      plan       <- cursor.downField(prov / "hadPlan").as[RunPlan]
    } yield Association(resourceId, agent, plan)
  }
}
