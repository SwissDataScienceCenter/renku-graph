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

package io.renku.eventlog.events.consumers.statuschange

import cats.syntax.all._
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults.ForProjects
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DBUpdateResultsSpec extends AnyWordSpec with should.Matchers {

  "combine" should {

    "merge status count for each project" in {
      val project1         = projectPaths.generateOne
      val project1Count    = EventStatus.all.map(_ -> Gen.choose(-200, 200).generateOne).toMap
      val project2         = projectPaths.generateOne
      val project2Count    = EventStatus.all.map(_ -> Gen.choose(-200, 200).generateOne).toMap
      val uniqueProject    = ForProjects(Set(project1 -> project1Count))
      val multipleProjects = ForProjects(Set(project1 -> project1Count, project2 -> project2Count))

      uniqueProject.combine(multipleProjects) shouldBe ForProjects(
        Set(project1 -> project1Count.combine(project1Count), project2 -> project2Count)
      )
    }
  }

  "DBUpdateResults.ForProjects.apply" should {

    "return the statuses count for the given project" in {
      val project1      = projectPaths.generateOne
      val project1Count = EventStatus.all.map(_ -> Gen.choose(-200, 200).generateOne).toMap
      val project2      = projectPaths.generateOne
      val project2Count = EventStatus.all.map(_ -> Gen.choose(-200, 200).generateOne).toMap

      val updateResults = ForProjects(Set(project1 -> project1Count, project2 -> project2Count))

      updateResults(project2) shouldBe project2Count
    }

    "return no statuses count if the given project does not exists in the results" in {
      ForProjects.empty(projectPaths.generateOne) shouldBe Map.empty
    }
  }
}
