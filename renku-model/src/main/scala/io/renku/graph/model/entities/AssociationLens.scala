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
import monocle.Lens

object AssociationLens {

  val associationPlan: Lens[Association, StepPlan] =
    Lens[Association, StepPlan](_.plan)(p => a => a.fold[Association](_.copy(plan = p))(_.copy(plan = p)))

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
}
