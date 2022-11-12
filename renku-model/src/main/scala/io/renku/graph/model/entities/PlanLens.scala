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

import io.renku.graph.model.plans.DateCreated
import monocle.Lens

object PlanLens {

  val stepPlanCreators: Lens[StepPlan, List[Person]] = Lens[StepPlan, List[Person]](_.creators) { persons =>
    {
      case plan: StepPlan.NonModified => plan.copy(creators = persons)
      case plan: StepPlan.Modified    => plan.copy(creators = persons)
    }
  }

  val planCreators: Lens[Plan, List[Person]] = Lens[Plan, List[Person]](_.creators) { persons =>
    { case plan: StepPlan => stepPlanCreators.modify(_ => persons)(plan) }
  }

  val planDateCreated: Lens[Plan, DateCreated] = Lens[Plan, DateCreated](_.dateCreated) { date =>
    {
      case plan: StepPlan.NonModified => plan.copy(dateCreated = date)
      case plan: StepPlan.Modified    => plan.copy(dateCreated = date)
    }
  }
}
