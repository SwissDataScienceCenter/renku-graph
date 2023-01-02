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

package io.renku.graph.model.entities

import cats.syntax.all._
import monocle.Lens

object AssociationLens {

  val associationAgent: Lens[Association, Either[Agent, Person]] =
    Lens[Association, Either[Agent, Person]](_.fold(_.agent.asRight[Agent])(_.agent.asLeft[Person])) {
      case Right(person) => {
        case assoc: Association.WithPersonAgent =>
          assoc.copy(agent = person)
        case Association.WithRenkuAgent(resourceId, _, plan) =>
          Association.WithPersonAgent(resourceId, person, plan)
      }
      case Left(agent) => {
        case Association.WithPersonAgent(resourceId, _, plan) =>
          Association.WithRenkuAgent(resourceId, agent, plan)
        case assoc: Association.WithRenkuAgent =>
          assoc.copy(agent = agent)
      }
    }

  def associationStepPlan(stepPlans: List[StepPlan]): Lens[Association, StepPlan] =
    Lens[Association, StepPlan](a =>
      stepPlans
        .find(_.resourceId == a.planId)
        .getOrElse(
          throw new IllegalStateException(s"Association ${a.resourceId} pointing to non-existing plan ${a.planId}")
        )
    )(p => {
      case a: Association.WithPersonAgent => a.copy(planId = p.resourceId)
      case a: Association.WithRenkuAgent  => a.copy(planId = p.resourceId)
    })
}
