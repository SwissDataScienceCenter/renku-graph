/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.cli.model.CliAssociation
import io.renku.graph.model._
import io.renku.graph.model.cli.CliConverters
import io.renku.jsonld._
import monocle.Lens

sealed trait Association {
  type AgentType
  val activity: Activity
  val agent:    AgentType
  val plan:     StepPlan

  def agentOrPerson: Either[Agent, Person]

  def fold[A](f1: Association.WithRenkuAgent => A, f2: Association.WithPersonAgent => A): A
}

object Association {

  final case class WithRenkuAgent(activity: Activity, agent: Agent, plan: StepPlan) extends Association {
    type AgentType = Agent

    def agentOrPerson: Either[Agent, Person] = Left(agent)

    def fold[A](f1: Association.WithRenkuAgent => A, f2: Association.WithPersonAgent => A): A = f1(this)
  }
  final case class WithPersonAgent(activity: Activity, agent: Person, plan: StepPlan) extends Association {
    type AgentType = Person
    def agentOrPerson: Either[Agent, Person] = Right(agent)

    def fold[A](f1: Association.WithRenkuAgent => A, f2: Association.WithPersonAgent => A): A = f2(this)
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

  implicit def toCliAssociation(implicit renkuUrl: RenkuUrl): Association => CliAssociation =
    CliConverters.from(_)

  implicit def encoder(implicit renkuUrl: RenkuUrl, graph: GraphClass): JsonLDEncoder[Association] =
    JsonLDEncoder.instance(_.to[entities.Association].asJsonLD)

  implicit def entityIdEncoder[A <: Association](implicit renkuUrl: RenkuUrl): EntityIdEncoder[A] =
    EntityIdEncoder.instance(_.activity.asEntityId.asUrlEntityId / "association")

  object Lenses {
    val plan: Lens[Association, StepPlan] = Lens[Association, StepPlan](_.plan)(p =>
      a =>
        a.fold(
          _.copy(plan = p),
          _.copy(plan = p)
        )
    )

    val agent: Lens[Association, Either[Agent, Person]] =
      Lens[Association, Either[Agent, Person]](_.agentOrPerson) {
        case Right(person) =>
          a => a.fold(p => Association.WithPersonAgent(p.activity, person, p.plan), _.copy(agent = person))
        case Left(agent) =>
          a => a.fold(_.copy(agent = agent), p => Association.WithRenkuAgent(p.activity, agent, p.plan))
      }
  }
}
