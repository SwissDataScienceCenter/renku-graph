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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.subscriptions.SubscriptionDataProvisioning
import io.renku.eventlog.subscriptions.globalcommitsync.GlobalCommitSyncEvent.{CommitsCount, CommitsInfo}
import io.renku.eventlog.{CreatedDate, EventDate, InMemoryEventLogDbSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events.{CommitId, CompoundEventId, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.data.Completion

import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.HOURS
import java.time.{Duration, Instant}
import scala.util.Random

class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return the event with the latest eventDate" +
      s"when the subscription_category_sync_times table does not have rows for the $categoryName" in new TestCase {
        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe None

        val project0                 = consumerProjects.generateOne
        val (eventId0, oldEventDate) = genCommitIdAndDate(olderThanAWeek = true, project0.id)
        addEvent(eventId0, oldEventDate, project0.path)

        val project1                    = consumerProjects.generateOne
        val (eventId1, latestEventDate) = genCommitIdAndDate(olderThanAWeek = false, project1.id)
        addEvent(eventId1, latestEventDate, project1.path)

        givenTheLastSyncedDateIsUpdated(project1)

        currentTime.expects().returning(now)
        finder.popEvent().unsafeRunSync() shouldBe GlobalCommitSyncEvent(project1,
                                                                         CommitsInfo(CommitsCount(1),
                                                                                     CommitId(eventId1.id.value)
                                                                         ),
                                                                         None
        ).some
      }

    "return an event with all commits " +
      s"when the project hasn't been synced yet " + // i.e. new project
      "and contains an event where the event_date is *MORE* than the syncFrequency ago " +
      "and the project's latest event is the most recent of all ready-to-sync projects" in new TestCase {

        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe None

        val project0                               = consumerProjects.generateOne
        val (project0Event0Id, project0Event0Date) = genCommitIdAndDate(olderThanAWeek = true, project0.id)
        val (project0Event1Id, project0Event1Date) = genCommitIdAndDate(olderThanAWeek = false, project0.id)
        addEvent(project0Event0Id, project0Event0Date, project0.path)
        addEvent(project0Event1Id, project0Event1Date, project0.path)

        val project1                               = consumerProjects.generateOne
        val (project1Event0Id, project1Event0Date) = genCommitIdAndDate(olderThanAWeek = true, project1.id)
        val (project1Event1Id, project1Event1Date) = genCommitIdAndDate(olderThanAWeek = true, project1.id)
        addEvent(project1Event0Id, project1Event0Date, project1.path)
        addEvent(project1Event1Id, project1Event1Date, project1.path)

        givenTheLastSyncedDateIsUpdated(project0)

        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          GlobalCommitSyncEvent(project0, CommitsInfo(CommitsCount(2), CommitId(project0Event1Id.id.value)), None)
        )
      }

    "return None " +
      s"when all projects were synced less than the syncFrequency ago" in new TestCase {
        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe None

        val project0           = consumerProjects.generateOne
        val project0LastSynced = genLastSynced(false)

        val (project0Event0Id, project0Event0Date) = genCommitIdAndDate(projectId = project0.id)
        val (project0Event1Id, project0Event1Date) = genCommitIdAndDate(projectId = project0.id)
        addEvent(project0Event0Id, project0Event0Date, project0.path)
        addEvent(project0Event1Id, project0Event1Date, project0.path)
        upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return an event with all commit ids for the project" +
      s"where the project hasn't been synced in less than the syncFrequency" +
      s"and the project's latest event is the most recent of all ready-to-sync projects" in new TestCase {
        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe None

        val project0           = consumerProjects.generateOne
        val project0LastSynced = genLastSynced(true)

        val (project0Event0Id, project0Event0Date) = genCommitIdAndDate(olderThanAWeek = true, projectId = project0.id)
        val (project0Event1Id, project0Event1Date) = genCommitIdAndDate(olderThanAWeek = false, projectId = project0.id)
        addEvent(project0Event0Id, project0Event0Date, project0.path)
        addEvent(project0Event1Id, project0Event1Date, project0.path)
        upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

        val project1           = consumerProjects.generateOne
        val project1LastSynced = genLastSynced(true)

        val project1Event0Id   = genCompoundEventId(projectId = project1.id)
        val project1Event0Date = EventDate(project0Event0Date.value.minus(1, ChronoUnit.DAYS))
        addEvent(project1Event0Id, project1Event0Date, project1.path)
        upsertCategorySyncTime(project1.id, categoryName, project1LastSynced)

        givenTheLastSyncedDateIsUpdated(project0)

        currentTime.expects().returning(now)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          GlobalCommitSyncEvent(project0,
                                CommitsInfo(CommitsCount(2), CommitId(project0Event1Id.id.value)),
                                project0LastSynced.some
          )
        )
      }

    "return an event with all commit ids for the project which are *NOT* AWAITING_DELETION or DELETING" +
      s"where the project hasn't been synced in less than the syncFrequency" +
      s"and the project's latest event is the most recent of all ready-to-sync projects" in new TestCase {

        currentTime.expects().returning(now)
        finder.popEvent().unsafeRunSync() shouldBe None

        val project0           = consumerProjects.generateOne
        val project0LastSynced = genLastSynced(true)

        val (project0Event0Id, project0Event0Date) = genCommitIdAndDate(olderThanAWeek = true, projectId = project0.id)
        val (project0Event1Id, project0Event1Date) = genCommitIdAndDate(olderThanAWeek = false, projectId = project0.id)
        val (project0Event2Id, project0Event2Date) = genCommitIdAndDate(olderThanAWeek = false, projectId = project0.id)
        addEvent(project0Event0Id, project0Event0Date, project0.path)
        addEvent(project0Event1Id, project0Event1Date, project0.path, eventStatus = AwaitingDeletion)
        addEvent(project0Event2Id, project0Event2Date, project0.path, eventStatus = Deleting)
        upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

        givenTheLastSyncedDateIsUpdated(project0)

        currentTime.expects().returning(now)
        finder.popEvent().unsafeRunSync() shouldBe GlobalCommitSyncEvent(project0,
                                                                         CommitsInfo(CommitsCount(1),
                                                                                     CommitId(project0Event0Id.id.value)
                                                                         ),
                                                                         project0LastSynced.some
        ).some
      }

    "return None " +
      "when all events are AWAITING_DELETION or DELETING but still update the last sync date of the project" in new TestCase {
        currentTime.expects().returning(now)
        finder.popEvent().unsafeRunSync() shouldBe None

        val project0           = consumerProjects.generateOne
        val project0LastSynced = genLastSynced(true)

        val (project0Event0Id, project0Event0Date) = genCommitIdAndDate(projectId = project0.id)
        val (project0Event1Id, project0Event1Date) = genCommitIdAndDate(projectId = project0.id)
        val (project0Event2Id, project0Event2Date) = genCommitIdAndDate(projectId = project0.id)
        addEvent(project0Event0Id, project0Event0Date, project0.path, eventStatus = AwaitingDeletion)
        addEvent(project0Event1Id, project0Event1Date, project0.path, eventStatus = AwaitingDeletion)
        addEvent(project0Event2Id, project0Event2Date, project0.path, eventStatus = Deleting)
        upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

        givenTheLastSyncedDateIsUpdated(project0)

        currentTime.expects().returning(now)
        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    val syncFrequency         = Duration ofDays 7
    val lastSyncedDateUpdater = mock[LastSyncedDateUpdater[IO]]
    val currentTime           = mockFunction[Instant]
    val now                   = Instant.now()
    val finder = new EventFinderImpl(lastSyncedDateUpdater,
                                     TestLabeledHistogram[SqlStatement.Name]("query_id"),
                                     syncFrequency,
                                     currentTime
    )

    def givenTheLastSyncedDateIsUpdated(project: Project) = {
      currentTime.expects().returning(now)
      (lastSyncedDateUpdater.run _)
        .expects(project.id, LastSyncedDate(now).some)
        .returning(Completion.Insert(1).pure[IO])
    }

    def genLastSynced(moreThanAWeekAgo: Boolean) =
      if (moreThanAWeekAgo)
        relativeTimestamps(moreThanAgo = syncFrequency.plus(2, HOURS)).generateAs(LastSyncedDate)
      else
        relativeTimestamps(lessThanAgo = syncFrequency.minus(2, HOURS)).generateAs(LastSyncedDate)
  }

  private def genCommitIdAndDate(olderThanAWeek: Boolean = Random.nextBoolean(),
                                 projectId:      projects.Id = projectIds.generateOne
  ) = {
    val eventDate =
      if (olderThanAWeek) relativeTimestamps(moreThanAgo = Duration.ofDays(8)).generateAs(EventDate)
      else relativeTimestamps(lessThanAgo = Duration.ofDays(6)).generateAs(EventDate)
    (genCompoundEventId(projectId), eventDate)
  }

  private def genCompoundEventId(projectId: projects.Id) = compoundEventIds.generateOne.copy(projectId = projectId)

  private def addEvent(
      eventId:     CompoundEventId,
      eventDate:   EventDate,
      projectPath: projects.Path,
      createdDate: CreatedDate = createdDates.generateOne,
      eventStatus: EventStatus =
        Gen.oneOf(EventStatus.all.filterNot(status => status == AwaitingDeletion || status == Deleting)).generateOne
  ): Unit = storeEvent(
    eventId,
    eventStatus,
    executionDates.generateOne,
    eventDate,
    eventBodies.generateOne,
    createdDate,
    projectPath = projectPath
  )
}
