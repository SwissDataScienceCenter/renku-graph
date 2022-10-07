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

object ActivityLens {
  val activityAssociation: Lens[Activity, Association] =
    Lens[Activity, Association](_.association)(assoc => act => act.copy(association = assoc))

  val activityAuthor: Lens[Activity, Person] =
    Lens[Activity, Person](_.author)(p => a => a.copy(author = p))

  val activityAssociationAgent: Lens[Activity, Either[Agent, Person]] =
    activityAssociation >>> AssociationLens.associationAgent

  val activityPlan: Lens[Activity, Plan] =
    activityAssociation >>> AssociationLens.associationPlan

  val activityPlanCreators: Lens[Activity, Set[Person]] =
    activityPlan >>> PlanLens.planCreators
}
