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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.graph.model._
import io.renku.jsonld._

sealed trait Association {
  type AgentType
  val activity: Activity
  val agent:    AgentType
  val plan:     StepPlan
}

object Association {

  final case class WithRenkuAgent(activity: Activity, agent: Agent, plan: StepPlan) extends Association {
    type AgentType = Agent
  }
  final case class WithPersonAgent(activity: Activity, agent: Person, plan: StepPlan) extends Association {
    type AgentType = Person
  }

  def factory(agent: Agent, plan: StepPlan): Activity => Association = Association.WithRenkuAgent(_, agent, plan)

  import io.renku.jsonld.syntax._

  implicit def toEntitiesAssociation(implicit renkuUrl: RenkuUrl): Association => entities.Association = {
    case a @ Association.WithRenkuAgent(_, agent, plan) =>
      entities.Association.WithRenkuAgent(associations.ResourceId(a.asEntityId.show),
                                          agent.to[entities.Agent],
                                          plan.to[entities.StepPlan].resourceId
      )
    case a @ Association.WithPersonAgent(_, agent, plan) =>
      entities.Association.WithPersonAgent(associations.ResourceId(a.asEntityId.show),
                                           agent.to[entities.Person],
                                           plan.to[entities.StepPlan].resourceId
      )
  }

  implicit def encoder(implicit renkuUrl: RenkuUrl, graph: GraphClass): JsonLDEncoder[Association] =
    JsonLDEncoder.instance(_.to[entities.Association].asJsonLD)

  implicit def entityIdEncoder[A <: Association](implicit renkuUrl: RenkuUrl): EntityIdEncoder[A] =
    EntityIdEncoder.instance(_.activity.asEntityId.asUrlEntityId / "association")
}
