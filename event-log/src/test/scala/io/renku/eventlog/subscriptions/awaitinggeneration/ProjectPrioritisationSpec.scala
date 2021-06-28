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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.EventDate
import io.renku.eventlog.subscriptions.{Capacity, ProjectIds, Subscribers}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant.now
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

private class ProjectPrioritisationSpec extends AnyWordSpec with should.Matchers with MockFactory {

  import ProjectPrioritisation._
  import Priority._

  "prioritize" should {

    "do not discard projects events if " +
      "current occupancy per project = 0, " +
      "total occupancy = 0, " +
      "total capacity = 10" in new TestCase {

        `given totalCapacity`(10)

        val projects = List(
          projectInfos.generateOne,
          projectInfos.generateOne
        ).map(_.copy(currentOccupancy = 0, latestEventDate = EventDate(now)))

        projectPrioritisation.prioritise(projects, totalOccupancy = 0).noPriority shouldBe projects.map(_.toIdsAndPath)
      }

    "do not discard projects events if " +
      "current occupancy per project > 0, " +
      "total occupancy < max free spots, " +
      "total capacity = 10" in new TestCase {

        `given totalCapacity`(10)

        val projects = List(
          projectInfos.generateOne,
          projectInfos.generateOne
        ).map(_.copy(currentOccupancy = positiveInts(max = 3).generateOne, latestEventDate = EventDate(now)))

        projectPrioritisation
          .prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)
          .noPriority shouldBe projects.map(_.toIdsAndPath)
      }

    "discard all project events with " +
      "current occupancy > 0, " +
      "total occupancy >= max free spots, " +
      "total capacity = 10" in new TestCase {

        `given totalCapacity`(10)

        val projects = List(
          projectInfos.generateOne.copy(currentOccupancy = 8),
          projectInfos.generateOne.copy(currentOccupancy = 1),
          projectInfos.generateOne.copy(currentOccupancy = 0)
        ).map(_.copy(latestEventDate = EventDate(now)))

        projectPrioritisation
          .prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)
          .noPriority shouldBe List(projects.last.toIdsAndPath)
      }

    "discard all project events with " +
      "current occupancy > 0," +
      "total occupancy >= max free spots, " +
      "total capacity = 10, and there are projects under processing not listed" in new TestCase {

        `given totalCapacity`(10)

        val projects = List(
          projectInfos.generateOne.copy(currentOccupancy = 1),
          projectInfos.generateOne.copy(currentOccupancy = 1),
          projectInfos.generateOne.copy(currentOccupancy = 0)
        ).map(_.copy(latestEventDate = EventDate(now)))

        projectPrioritisation
          .prioritise(projects, totalOccupancy = 8)
          .noPriority shouldBe List(projects.last.toIdsAndPath)
      }

    "discard all project events with " +
      "current occupancy > 0, " +
      "total occupancy >= max free spots, " +
      "total capacity = 40 (mutli-projects)" in new TestCase {

        `given totalCapacity`(40)

        val project0 = projectInfos.generateOne.copy(currentOccupancy = 22)
        val project1 = projectInfos.generateOne.copy(currentOccupancy = 1, latestEventDate = EventDate(now()))
        val project2 = projectInfos.generateOne.copy(currentOccupancy = 1, latestEventDate = EventDate(now()))
        val project3 = projectInfos.generateOne.copy(currentOccupancy = 12)

        val projectsAwaitingGeneration = projectInfos.generateFixedSizeList(4).map(_.copy(currentOccupancy = 0))

        val projects = List(project0, project1, project2, project3) ++ projectsAwaitingGeneration

        projectPrioritisation
          .prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)
          .noPriority shouldBe projectsAwaitingGeneration.map(_.toIdsAndPath)
      }

    "return an empty list if there are no projects" in new TestCase {
      `given no totalCapacity`

      projectPrioritisation.prioritise(Nil, totalOccupancy = positiveLongs().generateOne) shouldBe Nil
    }

    "put MaxPriority if the list contains only one project" in new TestCase {
      `given no totalCapacity`

      val project = projectInfos.generateOne

      projectPrioritisation.prioritise(List(project), totalOccupancy = positiveLongs().generateOne) shouldBe List(
        (project.toIdsAndPath, MaxPriority)
      )
    }

    "put priority MaxPriority if projects' event dates are within and hour and currentOccupancy is 0" in new TestCase {
      `given no totalCapacity`

      val project1 = projectInfos.generateOne.copy(currentOccupancy = 0)
      val project2 = projectInfos.generateOne.copy(
        latestEventDate = project1.latestEventDate.plus(durations(max = 59 minutes).generateOne),
        currentOccupancy = 0
      )

      projectPrioritisation.prioritise(List(project1, project2), totalOccupancy = 0) shouldBe List(
        (project1.toIdsAndPath, MaxPriority),
        (project2.toIdsAndPath, MaxPriority)
      )
    }

    "put the MinPriority to the project with the older events" +
      "if projects event dates are not within an hour and currentOccupancy is 0" in new TestCase {
        `given no totalCapacity`

        val project1 = projectInfos.generateOne.copy(currentOccupancy = 0)
        val project2 = projectInfos.generateOne.copy(
          latestEventDate = project1.latestEventDate.plus(durations(min = 61 minutes, max = 5 hours).generateOne),
          currentOccupancy = 0
        )

        projectPrioritisation.prioritise(List(project1, project2), totalOccupancy = 0) shouldBe List(
          (project1.toIdsAndPath, MaxPriority),
          (project2.toIdsAndPath, MinPriority)
        )
      }

    "compute the priority using the event date only " +
      "if projects' event dates are not within an hour and currentOccupancy is 0" in new TestCase {
        `given no totalCapacity`

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
          projectPrioritisation.prioritise(List(project1, project2, project3), totalOccupancy = 0)

        projectIdAndPath1.id shouldBe project1.id
        projectIdAndPath2.id shouldBe project2.id
        projectIdAndPath3.id shouldBe project3.id

        priority1.value shouldBe BigDecimal(1.0)
        priority2.value   should (be < BigDecimal(1.0) and be > BigDecimal(0.1))
        priority3.value shouldBe BigDecimal(0.1)
      }

    "put MinPriority to one of the projects " +
      "if projects' event dates are within an hour and they have the same currentOccupancy > 0" in new TestCase {
        `given no totalCapacity`

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

        val projects = List(project1, project2, project3)
        val (projectIdAndPath, priority) :: Nil =
          projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

        List(projectIdAndPath.id) should contain atLeastOneElementOf List(project1.id, project2.id, project3.id)
        priority                shouldBe MinPriority
      }

    "put MaxPriority to the project with current occupancy 0 while other has > 0" in new TestCase {
      `given no totalCapacity`

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

      val projects = List(project1, project2, project3)
      val (projectIdAndPath, priority) :: Nil =
        projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

      projectIdAndPath.id shouldBe project2.id
      priority            shouldBe MaxPriority
    }

    "discard project with too high occupancy " +
      "- case when projects' event dates are within an hour" in new TestCase {
        `given no totalCapacity`

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

        val projects = List(project1, project2, project3)
        val (projectIdAndPath2, priority2) :: (projectIdAndPath3, priority3) :: Nil =
          projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

        projectIdAndPath2.id shouldBe project2.id
        projectIdAndPath3.id shouldBe project3.id

        priority2.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
        priority3.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
      }

    "discard project with too high occupancy " +
      "- case when projects' event dates are not within an hour" in new TestCase {
        `given no totalCapacity`

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

        val projects = List(project1, project2, project3)
        val (projectIdAndPath1, priority1) :: (projectIdAndPath3, priority3) :: Nil =
          projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

        projectIdAndPath1.id shouldBe project1.id
        projectIdAndPath3.id shouldBe project3.id

        priority1     shouldBe MaxPriority
        priority3.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
      }
  }

  private trait TestCase {
    val subscribers                = mock[Subscribers[Try]]
    lazy val projectPrioritisation = new ProjectPrioritisationImpl(subscribers)

    def `given no totalCapacity` =
      (() => subscribers.getTotalCapacity)
        .expects()
        .returning(None)
        .atLeastOnce()

    def `given totalCapacity`(capacity: Int) =
      (() => subscribers.getTotalCapacity)
        .expects()
        .returning(Capacity(capacity).some)
        .atLeastOnce()
  }

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

  private implicit class ResultsOps(results: List[(ProjectIds, Priority)]) {
    lazy val noPriority: List[ProjectIds] = results.map(_._1)
  }
}
