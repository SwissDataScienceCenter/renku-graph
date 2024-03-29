/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.events.producers.DefaultSubscribers.DefaultSubscribers
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.events.EventDate
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant.now
import scala.concurrent.duration._
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

        val infos = List(
          projectInfos.generateOne,
          projectInfos.generateOne
        ).map(_.copy(currentOccupancy = 0, latestEventDate = EventDate(now)))

        projectPrioritisation.prioritise(infos, totalOccupancy = 0).noPriority shouldBe
          infos.map(i => ProjectIds(i.project))
      }

    "do not discard projects events if " +
      "current occupancy per project > 0, " +
      "total occupancy < max free spots, " +
      "total capacity = 10" in new TestCase {

        `given totalCapacity`(10)

        val infos = List(
          projectInfos.generateOne,
          projectInfos.generateOne
        ).map(_.copy(currentOccupancy = positiveInts(max = 3).generateOne, latestEventDate = EventDate(now)))

        projectPrioritisation
          .prioritise(infos, totalOccupancy = infos.map(_.currentOccupancy.value).sum)
          .noPriority shouldBe infos.map(i => ProjectIds(i.project))
      }

    "discard all project events with " +
      "current occupancy > 0, " +
      "total occupancy >= max free spots, " +
      "total capacity = 10" in new TestCase {

        `given totalCapacity`(10)

        val infos = List(
          projectInfos.generateOne.copy(currentOccupancy = 8),
          projectInfos.generateOne.copy(currentOccupancy = 1),
          projectInfos.generateOne.copy(currentOccupancy = 0)
        ).map(_.copy(latestEventDate = EventDate(now)))

        projectPrioritisation
          .prioritise(infos, totalOccupancy = infos.map(_.currentOccupancy.value).sum)
          .noPriority shouldBe List(ProjectIds(infos.last.project))
      }

    "discard all project events with " +
      "current occupancy > 0," +
      "total occupancy >= max free spots, " +
      "total capacity = 10, and there are projects under processing not listed" in new TestCase {

        `given totalCapacity`(10)

        val infos = List(
          projectInfos.generateOne.copy(currentOccupancy = 1),
          projectInfos.generateOne.copy(currentOccupancy = 1),
          projectInfos.generateOne.copy(currentOccupancy = 0)
        ).map(_.copy(latestEventDate = EventDate(now)))

        projectPrioritisation
          .prioritise(infos, totalOccupancy = 8)
          .noPriority shouldBe List(ProjectIds(infos.last.project))
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

        val infos = List(project0, project1, project2, project3) ++ projectsAwaitingGeneration

        projectPrioritisation
          .prioritise(infos, totalOccupancy = infos.map(_.currentOccupancy.value).sum)
          .noPriority shouldBe projectsAwaitingGeneration.map(i => ProjectIds(i.project))
      }

    "return an empty list if there are no projects" in new TestCase {
      `given no totalCapacity`

      projectPrioritisation.prioritise(Nil, totalOccupancy = positiveLongs().generateOne) shouldBe Nil
    }

    "put MaxPriority if the list contains only one project" in new TestCase {
      `given no totalCapacity`

      val project = projectInfos.generateOne

      projectPrioritisation.prioritise(List(project), totalOccupancy = positiveLongs().generateOne) shouldBe
        List((ProjectIds(project.project), MaxPriority))
    }

    "put priority MaxPriority if projects' event dates are within and hour and currentOccupancy is 0" in new TestCase {
      `given no totalCapacity`

      val project1 = projectInfos.generateOne.copy(currentOccupancy = 0)
      val project2 = projectInfos.generateOne.copy(
        latestEventDate = project1.latestEventDate.plus(durations(max = 59 minutes).generateOne),
        currentOccupancy = 0
      )

      projectPrioritisation.prioritise(List(project1, project2), totalOccupancy = 0) shouldBe List(
        (ProjectIds(project1.project), MaxPriority),
        (ProjectIds(project2.project), MaxPriority)
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
          (ProjectIds(project1.project), MaxPriority),
          (ProjectIds(project2.project), MinPriority)
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

        val (projectIdAndSlug1, priority1) :: (projectIdAndSlug2, priority2) :: (projectIdAndSlug3, priority3) :: Nil =
          projectPrioritisation.prioritise(List(project1, project2, project3), totalOccupancy = 0)

        projectIdAndSlug1.id shouldBe project1.project.id
        projectIdAndSlug2.id shouldBe project2.project.id
        projectIdAndSlug3.id shouldBe project3.project.id

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
        val (projectIdAndSlug, priority) :: Nil =
          projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

        List(projectIdAndSlug.id) should contain atLeastOneElementOf List(project1.project.id,
                                                                          project2.project.id,
                                                                          project3.project.id
        )
        priority shouldBe MinPriority
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
      val (projectIdAndSlug, priority) :: Nil =
        projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

      projectIdAndSlug.id shouldBe project2.project.id
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
        val (projectIdAndSlug2, priority2) :: (projectIdAndSlug3, priority3) :: Nil =
          projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

        projectIdAndSlug2.id shouldBe project2.project.id
        projectIdAndSlug3.id shouldBe project3.project.id

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
        val (projectIdAndSlug1, priority1) :: (projectIdAndSlug3, priority3) :: Nil =
          projectPrioritisation.prioritise(projects, totalOccupancy = projects.map(_.currentOccupancy.value).sum)

        projectIdAndSlug1.id shouldBe project1.project.id
        projectIdAndSlug3.id shouldBe project3.project.id

        priority1     shouldBe MaxPriority
        priority3.value should (be <= BigDecimal(1.0) and be >= BigDecimal(0.1))
      }
  }

  private trait TestCase {

    implicit val subscribers: DefaultSubscribers[Try] = mock[DefaultSubscribers[Try]]
    lazy val projectPrioritisation = new ProjectPrioritisationImpl[Try]

    def `given no totalCapacity` =
      (() => subscribers.getTotalCapacity)
        .expects()
        .returning(None)
        .atLeastOnce()

    def `given totalCapacity`(capacity: Int) =
      (() => subscribers.getTotalCapacity)
        .expects()
        .returning(TotalCapacity(capacity).some)
        .atLeastOnce()
  }

  private implicit lazy val projectInfos: Gen[ProjectInfo] = for {
    project          <- consumerProjects
    latestEvenDate   <- eventDates
    currentOccupancy <- nonNegativeInts(20)
  } yield ProjectInfo(project, latestEvenDate, currentOccupancy)

  private implicit class EventDateOps(eventDate: EventDate) {
    def plus(duration: FiniteDuration): EventDate =
      EventDate(eventDate.value plusMillis duration.toMillis)
  }

  private implicit class ResultsOps(results: List[(ProjectIds, Priority)]) {
    lazy val noPriority: List[ProjectIds] = results.map(_._1)
  }
}
