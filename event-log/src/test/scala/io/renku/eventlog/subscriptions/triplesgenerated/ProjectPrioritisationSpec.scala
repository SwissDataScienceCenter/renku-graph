/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.triplesgenerated

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.EventDate
import io.renku.eventlog.subscriptions.ProjectIds
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

private class ProjectPrioritisationSpec extends AnyWordSpec with should.Matchers {
  import ProjectPrioritisation._
  import Priority._

  "prioritize" should {

    "return an empty list if there are no projects" in {
      projectPrioritisation.prioritise(Nil) shouldBe Nil
    }

    "put priority MaxPriority if the list contains only one project" in {
      val project = projectInfos.generateOne

      projectPrioritisation.prioritise(List(project)) shouldBe List((project.toIdsAndPath, MaxPriority))
    }

    "compute the priority using the event date only " in {
      val project1 = projectInfos.generateOne
      val project2 = projectInfos.generateOne.copy(
        latestEventDate = project1.latestEventDate.plus(durations(min = 61 minutes, max = 5 hours).generateOne),
        currentOccupancy = 0
      )
      val project3 = projectInfos.generateOne.copy(
        latestEventDate = project2.latestEventDate.plus(durations(min = 61 minutes, max = 5 hours).generateOne),
        currentOccupancy = 0
      )

      val (projectIdAndPath1, priority1) :: (projectIdAndPath2, priority2) :: (projectIdAndPath3, priority3) :: Nil =
        projectPrioritisation.prioritise(List(project1, project2, project3))

      projectIdAndPath1.id shouldBe project1.id
      projectIdAndPath2.id shouldBe project2.id
      projectIdAndPath3.id shouldBe project3.id

      priority1.value shouldBe BigDecimal(1.0)
      priority2.value   should (be < BigDecimal(1.0) and be > BigDecimal(0.1))
      priority3.value shouldBe BigDecimal(0.1)
    }
  }

  private lazy val projectPrioritisation = new ProjectPrioritisation()

  private implicit lazy val projectInfos: Gen[ProjectInfo] = for {
    projectId        <- projectIds
    projectPath      <- projectPaths
    latestEvenDate   <- eventDates
    currentOccupancy <- nonNegativeInts(20)
  } yield ProjectInfo(projectId, projectPath, latestEvenDate, currentOccupancy)

  private implicit class EventDateOps(eventDate: EventDate) {
    def plus(duration: FiniteDuration): EventDate =
      EventDate(eventDate.value plusMillis duration.toMillis)
  }

  private implicit class ProjectInfoOps(projectInfo: ProjectInfo) {
    lazy val toIdsAndPath = ProjectIds(projectInfo.id, projectInfo.path)
  }
}
