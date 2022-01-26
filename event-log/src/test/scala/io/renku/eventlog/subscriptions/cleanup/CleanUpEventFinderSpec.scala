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

package io.renku.eventlog.subscriptions.cleanup
import cats.effect.IO
import io.renku.eventlog._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import io.renku.eventlog.subscriptions.cleanup.CleanUpEventFinderImpl
import io.renku.metrics.TestLabeledHistogram
import io.renku.db.SqlStatement
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import eu.timepit.refined.auto._
import EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.metrics.LabeledGauge
import java.time.Instant
import io.renku.graph.model.projects
import io.renku.graph.model.events.CompoundEventId
import org.scalacheck.Gen
import io.renku.events.consumers.Project
import java.time.temporal.ChronoUnit

class CleanUpEventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "popEvent" should {
    s"find the project with the oldest event in status $AwaitingDeletion and " +
      s"mark all awaiting deletion event of the project as $Deleting" in new TestCase {
        val projectId   = projectIds.generateOne
        val projectPath = projectPaths.generateOne

        val (event1Id, notOldestExecutionDate) = createEvent(
          status = AwaitingDeletion,
          executionDate = timestamps(max = now).generateAs(ExecutionDate),
          projectId = projectId,
          projectPath = projectPath
        )

        val (event2Id, oldestExecutionDate) = createEvent(
          status = AwaitingDeletion,
          executionDate = timestamps(max = notOldestExecutionDate.value).generateAs(ExecutionDate),
          projectId = projectId,
          projectPath = projectPath
        )

        val (event3Id, _) = createEvent(
          status =
            Gen.oneOf(EventStatus.all.filter(status => status != AwaitingDeletion && status != Deleting)).generateOne,
          executionDate = timestamps(max = now).generateAs(ExecutionDate),
          projectId = projectId,
          projectPath = projectPath
        )

        val project2Id   = projectIds.generateOne
        val project2Path = projectPaths.generateOne
        val (event4id, _) = createEvent(
          status = AwaitingDeletion,
          executionDate = timestamps(min = oldestExecutionDate.value.minus(5, ChronoUnit.MINUTES), max = now)
            .generateAs(ExecutionDate),
          projectId = project2Id,
          projectPath = project2Path
        )

        findEvents(Deleting) shouldBe List.empty

        expectAwaitingDeletionGaugeUpdate(projectPath, -2)
        expectDeletingGaugeUpdate(projectPath, 2)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          CleanUpEvent(Project(projectId, projectPath))
        )

        findEvents(Deleting).noBatchDate shouldBe List((event1Id, executionDate), (event2Id, executionDate))

        expectAwaitingDeletionGaugeUpdate(project2Path, -1)
        expectDeletingGaugeUpdate(project2Path, 1)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          CleanUpEvent(Project(project2Id, project2Path))
        )

        finder.popEvent().unsafeRunSync() shouldBe None

        queriesExecTimes.verifyExecutionTimeMeasured("clean_up - find oldest", "clean_up - update status")

      }

    "return no event if the execution date is in the future" in new TestCase {
      val projectId   = projectIds.generateOne
      val projectPath = projectPaths.generateOne

      createEvent(
        status = AwaitingDeletion,
        executionDate = timestamps(min = now.plus(5, ChronoUnit.MINUTES)).generateAs(ExecutionDate),
        projectId = projectId,
        projectPath = projectPath
      )

      findEvents(Deleting) shouldBe List.empty

      finder.popEvent().unsafeRunSync() shouldBe None

      findEvents(Deleting).noBatchDate shouldBe List.empty

      queriesExecTimes.verifyExecutionTimeMeasured("clean_up - find oldest")
    }
  }

  private trait TestCase {
    val now           = Instant.now()
    val executionDate = ExecutionDate(now)
    val currentTime   = mockFunction[Instant]
    currentTime.expects().returning(now).anyNumberOfTimes()

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")

    val awatingDeletionGauge = mock[LabeledGauge[IO, projects.Path]]
    val deletingGauge        = mock[LabeledGauge[IO, projects.Path]]

    val finder =
      new CleanUpEventFinderImpl(sessionResource, awatingDeletionGauge, deletingGauge, queriesExecTimes, currentTime)

    def expectAwaitingDeletionGaugeUpdate(projectPath: projects.Path, count: Double) =
      (awatingDeletionGauge.update _)
        .expects((projectPath, count))
        .returning(IO.unit)

    def expectDeletingGaugeUpdate(projectPath: projects.Path, count: Double) =
      (deletingGauge.update _)
        .expects((projectPath, count))
        .returning(IO.unit)

    def createEvent(status:        EventStatus,
                    executionDate: ExecutionDate,
                    projectId:     projects.Id = projectIds.generateOne,
                    projectPath:   projects.Path = projectPaths.generateOne
    ): (CompoundEventId, ExecutionDate) = {
      val eventId   = compoundEventIds.generateOne.copy(projectId = projectId)
      val eventBody = eventBodies.generateOne
      val batchDate = batchDates.generateOne
      val eventDate = eventDates.generateOne

      storeEvent(eventId, status, executionDate, eventDate, eventBody, batchDate = batchDate, projectPath = projectPath)

      (eventId, executionDate)
    }

  }
}
