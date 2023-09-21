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

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class PlanLensSpec extends AnyWordSpec with should.Matchers with EntitiesGenerators {

  "planCreators" should {
    "get and set" in {
      val p1 = createPlan
      val p2 = createPlan

      PlanLens.planCreators.get(p1) shouldBe p1.creators
      PlanLens.planCreators.replace(p2.creators)(p1) shouldBe {
        p1 match {
          case p: StepPlan.NonModified      => p.copy(creators = p2.creators)
          case p: StepPlan.Modified         => p.copy(creators = p2.creators)
          case p: CompositePlan.NonModified => p.copy(creators = p2.creators)
          case p: CompositePlan.Modified    => p.copy(creators = p2.creators)
        }
      }
    }
  }

  private def createPlan =
    stepPlanEntities()
      .apply(GraphModelGenerators.projectCreatedDates().generateOne)
      .generateOne
      .to[Plan]
}
