/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.unprocessed

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.DbEventLogGenerators._
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

    "put priority MaxPriority if projects' event dates are within and hour and currentOccupancy is 0" in {
      val project1 = projectInfos.generateOne.copy(currentOccupancy = 0)
      val project2 = projectInfos.generateOne.copy(
        latestEventDate = project1.latestEventDate.plus(durations(max = 59 minutes).generateOne),
        currentOccupancy = 0
      )

      projectPrioritisation.prioritise(List(project1, project2)) shouldBe List(
        (project1.toIdsAndPath, MaxPriority),
        (project2.toIdsAndPath, MaxPriority)
      )
    }

    "put the MinPriority to the project with the older events" +
      "if projects event dates are not within an hour and currentOccupancy is 0" in {
        val project1 = projectInfos.generateOne.copy(currentOccupancy = 0)
        val project2 = projectInfos.generateOne.copy(
          latestEventDate = project1.latestEventDate.plus(durations(min = 61 minutes, max = 5 hours).generateOne),
          currentOccupancy = 0
        )

        projectPrioritisation.prioritise(List(project1, project2)) shouldBe List(
          (project1.toIdsAndPath, MaxPriority),
          (project2.toIdsAndPath, MinPriority)
        )
      }

    "compute the priority using the event date only " +
      "if projects' event dates are not within an hour and currentOccupancy is 0" in {
        val project1 = projectInfos.generateOne.copy(currentOccupancy = 0)
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

    "put MinPriority to one of the projects " +
      "if projects' event dates are within an hour and they have the same currentOccupancy > 0" in {
        val currentOccupancy: Int Refined NonNegative = 1
        val project1 = projectInfos.generateOne.copy(currentOccupancy = currentOccupancy)
        val project2 = projectInfos.generateOne.copy(
          latestEventDate = project1.latestEventDate.plus(durations(max = 30 minutes).generateOne),
          currentOccupancy = currentOccupancy
        )
        val project3 = projectInfos.generateOne.copy(
          latestEventDate = project2.latestEventDate.plus(durations(max = 29 minutes).generateOne),
          currentOccupancy = currentOccupancy
        )

        val (projectIdAndPath, priority) :: Nil = projectPrioritisation.prioritise(List(project1, project2, project3))

        List(projectIdAndPath.id) should contain atLeastOneElementOf List(project1.id, project2.id, project3.id)
        priority                shouldBe MinPriority
      }

    "put MaxPriority to the project with current occupancy 0 while other has > 0" in {
      val currentOccupancy: Int Refined NonNegative = 1
      val project1 = projectInfos.generateOne.copy(currentOccupancy = currentOccupancy)
      val project2 = projectInfos.generateOne.copy(
        latestEventDate = project1.latestEventDate.plus(durations(max = 30 minutes).generateOne),
        currentOccupancy = 0
      )
      val project3 = projectInfos.generateOne.copy(
        latestEventDate = project2.latestEventDate.plus(durations(max = 29 minutes).generateOne),
        currentOccupancy = currentOccupancy
      )

      val (projectIdAndPath, priority) :: Nil = projectPrioritisation.prioritise(List(project1, project2, project3))

      projectIdAndPath.id shouldBe project2.id
      priority            shouldBe MaxPriority
    }

    "discard project with too high occupancy " +
      "- case when projects' event dates are within an hour" in {
        val project1 = projectInfos.generateOne.copy(
          currentOccupancy = 5
        )
        val project2 = projectInfos.generateOne.copy(
          latestEventDate = project1.latestEventDate.plus(durations(max = 30 minutes).generateOne),
          currentOccupancy = 0
        )
        val project3 = projectInfos.generateOne.copy(
          latestEventDate = project2.latestEventDate.plus(durations(max = 29 minutes).generateOne),
          currentOccupancy = 1
        )

        val (projectIdAndPath2, priority2) :: (projectIdAndPath3, priority3) :: Nil =
          projectPrioritisation.prioritise(List(project1, project2, project3))

        projectIdAndPath2.id shouldBe project2.id
        projectIdAndPath3.id shouldBe project3.id

        priority2.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
        priority3.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
      }

    "discard project with too high occupancy " +
      "- case when projects' event dates are not within an hour" in {
        val project1 = projectInfos.generateOne.copy(
          currentOccupancy = 1
        )
        val project2 = projectInfos.generateOne.copy(
          latestEventDate = project1.latestEventDate.plus(durations(min = 61 minutes, max = 5 hours).generateOne),
          currentOccupancy = 5
        )
        val project3 = projectInfos.generateOne.copy(
          latestEventDate = project2.latestEventDate.plus(durations(min = 61 minutes, max = 5 hours).generateOne),
          currentOccupancy = 0
        )

        val (projectIdAndPath1, priority1) :: (projectIdAndPath3, priority3) :: Nil =
          projectPrioritisation.prioritise(List(project1, project2, project3))

        projectIdAndPath1.id shouldBe project1.id
        projectIdAndPath3.id shouldBe project3.id

        priority1     shouldBe MaxPriority
        priority3.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
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
