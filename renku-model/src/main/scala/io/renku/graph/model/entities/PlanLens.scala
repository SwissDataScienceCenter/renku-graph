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

import cats.syntax.option._
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.entities.Plan.Derivation
import io.renku.graph.model.plans.DateCreated
import monocle.{Getter, Lens, Setter}

object PlanLens {

  val getModifiedProperties: Getter[Plan, Option[(Derivation, Option[InvalidationTime])]] =
    Getter[Plan, Option[(Derivation, Option[InvalidationTime])]] {
      case p: StepPlan.Modified         => (p.derivation, p.maybeInvalidationTime).some
      case p: CompositePlan.Modified    => (p.derivation, p.maybeInvalidationTime).some
      case _: StepPlan.NonModified      => None
      case _: CompositePlan.NonModified => None
    }

  val getPlanDerivation: Getter[Plan, Option[Derivation]] =
    Getter[Plan, Option[Derivation]](getModifiedProperties.get(_).map(_._1))

  val setPlanDerivation: Setter[Plan, Derivation] =
    Setter[Plan, Derivation](f => {
      case p: StepPlan.Modified         => p.copy(derivation = f(p.derivation))
      case p: CompositePlan.Modified    => p.copy(derivation = f(p.derivation))
      case p: StepPlan.NonModified      => p
      case p: CompositePlan.NonModified => p
    })

  val stepPlanCreators: Lens[StepPlan, List[Person]] = Lens[StepPlan, List[Person]](_.creators) { persons =>
    {
      case plan: StepPlan.NonModified => plan.copy(creators = persons)
      case plan: StepPlan.Modified    => plan.copy(creators = persons)
    }
  }

  val compositePlanCreators: Lens[CompositePlan, List[Person]] = Lens[CompositePlan, List[Person]](_.creators) {
    persons =>
      {
        case plan: CompositePlan.NonModified => plan.copy(creators = persons)
        case plan: CompositePlan.Modified    => plan.copy(creators = persons)
      }
  }

  val planCreators: Lens[Plan, List[Person]] = Lens[Plan, List[Person]](_.creators) { persons =>
    {
      case plan: StepPlan      => stepPlanCreators.modify(_ => persons)(plan)
      case plan: CompositePlan => compositePlanCreators.modify(_ => persons)(plan)
    }
  }

  val planDateCreated: Lens[Plan, DateCreated] = Lens[Plan, DateCreated](_.dateCreated) { date =>
    {
      case plan: StepPlan.NonModified      => plan.copy(dateCreated = date)
      case plan: StepPlan.Modified         => plan.copy(dateCreated = date)
      case plan: CompositePlan.NonModified => plan.copy(dateCreated = date)
      case plan: CompositePlan.Modified    => plan.copy(dateCreated = date)
    }
  }

  val planInvalidationTime: Lens[Plan, Option[InvalidationTime]] = Lens[Plan, Option[InvalidationTime]] {
    case _:    StepPlan.NonModified      => None
    case plan: StepPlan.Modified         => plan.maybeInvalidationTime
    case _:    CompositePlan.NonModified => None
    case plan: CompositePlan.Modified    => plan.maybeInvalidationTime
  } { maybeInvalidationTime =>
    {
      case plan: StepPlan.NonModified      => plan
      case plan: StepPlan.Modified         => plan.copy(maybeInvalidationTime = maybeInvalidationTime)
      case plan: CompositePlan.NonModified => plan
      case plan: CompositePlan.Modified    => plan.copy(maybeInvalidationTime = maybeInvalidationTime)
    }
  }
}
