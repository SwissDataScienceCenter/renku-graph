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

import monocle.Lens
import io.renku.graph.model.plans.{ResourceId => PlanResourceId}

object ProjectLens {

  val collectStepPlans: List[Plan] => List[StepPlan] = _.collect { case p: StepPlan => p }

  def plansLens[P <: Project]: Lens[P, List[Plan]] = Lens[P, List[Plan]](_.plans)(plans => {
    case p: RenkuProject.WithParent    => p.copy(plans = plans).asInstanceOf[P]
    case p: RenkuProject.WithoutParent => p.copy(plans = plans).asInstanceOf[P]
    case p => p
  })

  /** Recursively collects all existing [[StepPlan]]s of all containing [[CompositePlan]]s.
   * 
   * The known plans are given via the `all` map. If a plan is not available in this map, but
   * referenced in an composite plan, it will not be returned.
   */
  def collectAllSubPlans(all: Map[PlanResourceId, Plan])(start: Plan): Set[StepPlan] = {
    @annotation.tailrec
    def loop(result: Set[StepPlan], current: List[Plan]): Set[StepPlan] =
      current match {
        case Nil => result
        case (p: CompositePlan) :: rest =>
          loop(result, p.plans.toList.flatMap(all.get) ::: rest)
        case (p: StepPlan) :: rest =>
          loop(result + p, rest)
      }

    loop(Set.empty, List(start))
  }

  /** Recursively collects all existing [[CompositePlan]]s that are children of the given start plan.
   * 
   * The given `start` plan is not included in the result. Any plans that are referenced but don't 
   * exist in the given `all` map, are not returned. 
   */
  def collectAllSubCompositePlans(all: Map[PlanResourceId, Plan])(start: CompositePlan): Set[CompositePlan] = {
    @annotation.tailrec
    def loop(result: Set[CompositePlan], current: List[Plan]): Set[CompositePlan] =
      current match {
        case Nil => result
        case (p: CompositePlan) :: rest =>
          loop(result + p, p.plans.toList.flatMap(all.get) ::: rest)
        case (_: StepPlan) :: rest =>
          loop(result, rest)
      }

    loop(Set.empty, start.plans.toList.flatMap(all.get))
  }
}
