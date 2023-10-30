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

class ActivityLensSpec extends AnyWordSpec with should.Matchers with EntitiesGenerators {

  "activityAssociation" should {
    "get and set" in {
      val a1 = createActivity
      val a2 = createActivity
      ActivityLens.activityAssociation.get(a1)                     shouldBe a1.association
      ActivityLens.activityAssociation.replace(a2.association)(a1) shouldBe a1.copy(association = a2.association)
    }
  }

  "activityAuthor" should {
    "get and set" in {
      val activity = createActivity
      val person   = createActivity.author
      ActivityLens.activityAuthor.get(activity)             shouldBe activity.author
      ActivityLens.activityAuthor.replace(person)(activity) shouldBe activity.copy(author = person)
    }
  }

  private def createActivity =
    activityEntities(stepPlanEntities())
      .apply(GraphModelGenerators.projectCreatedDates().generateOne)
      .generateOne
      .to[Activity]
}
