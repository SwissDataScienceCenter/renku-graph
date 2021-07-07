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
import ch.datascience.graph.model.associations.ResourceId
import ch.datascience.graph.model.{agents, runPlans}
import io.circe.DecodingFailure
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class Association(resourceId: ResourceId, agent: Agent, runPlan: RunPlan)

object Association {

  val entityTypes: EntityTypes = EntityTypes of (prov / "Association")

  implicit lazy val encoder: JsonLDEncoder[Association] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      entity.resourceId.asEntityId,
      entityTypes,
      prov / "agent"   -> entity.agent.asJsonLD,
      prov / "hadPlan" -> entity.runPlan.resourceId.asEntityId.asJsonLD
    )
  }

  implicit lazy val decoder: JsonLDDecoder[Association] = JsonLDDecoder.entity(entityTypes) { cursor =>
    def multipleToNone[T]: List[T] => Either[DecodingFailure, Option[T]] = {
      case entity :: Nil => Right(Some(entity))
      case _             => Right(None)
    }

    for {
      resourceId      <- cursor.downEntityId.as[ResourceId]
      agentResourceId <- cursor.downField(prov / "agent").downEntityId.as[agents.ResourceId]
      agent <- cursor.top
                 .map(_.cursor.as[List[Agent]].map(_.filter(_.resourceId == agentResourceId)).flatMap(multipleToNone))
                 .flatMap(_.sequence)
                 .getOrElse(DecodingFailure(s"Association $resourceId without or with multiple Agents", Nil).asLeft)

      planResourceId <- cursor.downField(prov / "hadPlan").downEntityId.as[runPlans.ResourceId]
      plan <- cursor.top
                .map(_.cursor.as[List[RunPlan]].map(_.filter(_.resourceId == planResourceId)).flatMap(multipleToNone))
                .flatMap(_.sequence)
                .getOrElse(DecodingFailure(s"Association $resourceId without or with multiple RunPlans", Nil).asLeft)
    } yield Association(resourceId, agent, plan)
  }
}
