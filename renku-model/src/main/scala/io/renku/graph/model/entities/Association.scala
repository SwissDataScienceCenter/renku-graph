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

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.GitLabApiUrl
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.associations.ResourceId
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait Association {
  type AgentType
  val resourceId: ResourceId
  val agent:      AgentType
  val plan:       Plan
}

object Association {

  final case class WithRenkuAgent(resourceId: ResourceId, agent: Agent, plan: Plan) extends Association {
    type AgentType = Agent
  }
  final case class WithPersonAgent(resourceId: ResourceId, agent: Person, plan: Plan) extends Association {
    type AgentType = Person
  }

  val entityTypes: EntityTypes = EntityTypes of (prov / "Association")

  implicit def encoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Association] = JsonLDEncoder.instance {
    case WithRenkuAgent(resourceId, agent, plan) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        prov / "agent"   -> agent.asJsonLD,
        prov / "hadPlan" -> plan.resourceId.asEntityId.asJsonLD
      )
    case WithPersonAgent(resourceId, agent, plan) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        prov / "agent"   -> agent.asJsonLD,
        prov / "hadPlan" -> plan.resourceId.asEntityId.asJsonLD
      )
  }

  implicit lazy val decoder: JsonLDDecoder[Association] = JsonLDDecoder.entity(entityTypes) { implicit cursor =>
    for {
      resourceId <- cursor.downEntityId.as[ResourceId]
      plan       <- cursor.downField(prov / "hadPlan").as[Plan]
      association <- cursor.downField(prov / "agent").as[Option[Agent]] match {
                       case Right(Some(agent)) => Association.WithRenkuAgent(resourceId, agent, plan).asRight
                       case _                  => tryAsPersonAgent(resourceId, plan)
                     }
    } yield association
  }

  private def tryAsPersonAgent(resourceId: ResourceId, plan: Plan)(implicit cursor: Cursor) =
    cursor.downField(prov / "agent").as[Option[Person]] >>= {
      case Some(agent) => Association.WithPersonAgent(resourceId, agent, plan).asRight
      case None =>
        DecodingFailure(show"Association $resourceId without a valid ${prov / "agent"}", Nil).asLeft
    }
}
