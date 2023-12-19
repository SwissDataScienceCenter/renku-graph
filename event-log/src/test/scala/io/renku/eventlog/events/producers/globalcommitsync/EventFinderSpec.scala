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

package io.renku.eventlog.events.producers
package globalcommitsync

import GlobalCommitSyncEvent.{CommitsCount, CommitsInfo}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk.data.Completion

import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.HOURS
import java.time.{Duration, Instant}
import scala.util.Random

class EventFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  "popEvent" should {

    "return the event with the latest eventDate" +
      s"when the subscription_category_sync_times table does not have rows for the $categoryName" in testDBResource
        .use { implicit cfg =>
          for {
            _ <- finder.popEvent().asserting(_ shouldBe None)

            project0     = consumerProjects.generateOne
            oldEventDate = genEventDate(olderThanAWeek = true)
            _ <- addEvent(oldEventDate, project0)

            project1        = consumerProjects.generateOne
            latestEventDate = genEventDate(olderThanAWeek = false)
            event1 <- addEvent(latestEventDate, project1)

            _ = givenTheLastSyncedDateIsUpdated(project1)

            _ <- finder
                   .popEvent()
                   .asserting(
                     _ shouldBe GlobalCommitSyncEvent(project1,
                                                      CommitsInfo(CommitsCount(1), CommitId(event1.eventId.id.value)),
                                                      None
                     ).some
                   )
          } yield Succeeded
        }

    "return an event with all commits " +
      s"when the project hasn't been synced yet " + // i.e. new project
      "and contains an event where the event_date is *MORE* than the syncFrequency ago " +
      "and the project's latest event is the most recent of all ready-to-sync projects" in testDBResource.use {
        implicit cfg =>
          for {
            _ <- finder.popEvent().asserting(_ shouldBe None)

            project0           = consumerProjects.generateOne
            project0Event0Date = genEventDate(olderThanAWeek = true)
            project0Event1Date = genEventDate(olderThanAWeek = false)
            _              <- addEvent(project0Event0Date, project0)
            project0Event1 <- addEvent(project0Event1Date, project0)

            project1           = consumerProjects.generateOne
            project1Event0Date = genEventDate(olderThanAWeek = true)
            project1Event1Date = genEventDate(olderThanAWeek = true)
            _ <- addEvent(project1Event0Date, project1)
            _ <- addEvent(project1Event1Date, project1)

            _ = givenTheLastSyncedDateIsUpdated(project0)

            _ <- finder
                   .popEvent()
                   .asserting(
                     _ shouldBe Some(
                       GlobalCommitSyncEvent(project0,
                                             CommitsInfo(CommitsCount(2), CommitId(project0Event1.eventId.id.value)),
                                             None
                       )
                     )
                   )
          } yield Succeeded
      }

    "return None " +
      s"when all projects were synced less than the syncFrequency ago" in testDBResource.use { implicit cfg =>
        for {
          _ <- finder.popEvent().asserting(_ shouldBe None)

          project0           = consumerProjects.generateOne
          project0LastSynced = genLastSynced(false)
          project0Event0Date = genEventDate()
          project0Event1Date = genEventDate()
          _ <- addEvent(project0Event0Date, project0)
          _ <- addEvent(project0Event1Date, project0)
          _ <- upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield Succeeded
      }

    "return an event with all commit ids for the project" +
      s"where the project hasn't been synced in less than the syncFrequency" +
      s"and the project's latest event is the most recent of all ready-to-sync projects" in testDBResource.use {
        implicit cfg =>
          for {
            _ <- finder.popEvent().asserting(_ shouldBe None)

            project0           = consumerProjects.generateOne
            project0LastSynced = genLastSynced(true)
            project0Event0Date = genEventDate(olderThanAWeek = true)
            project0Event1Date = genEventDate(olderThanAWeek = false)
            _              <- addEvent(project0Event0Date, project0)
            project0Event1 <- addEvent(project0Event1Date, project0)
            _              <- upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

            project1           = consumerProjects.generateOne
            project1LastSynced = genLastSynced(true)
            project1Event0Date = EventDate(project0Event0Date.value.minus(1, ChronoUnit.DAYS))
            _ <- addEvent(project1Event0Date, project1)
            _ <- upsertCategorySyncTime(project1.id, categoryName, project1LastSynced)

            _ = givenTheLastSyncedDateIsUpdated(project0)

            _ <- finder
                   .popEvent()
                   .asserting(
                     _ shouldBe Some(
                       GlobalCommitSyncEvent(project0,
                                             CommitsInfo(CommitsCount(2), CommitId(project0Event1.eventId.id.value)),
                                             project0LastSynced.some
                       )
                     )
                   )
          } yield Succeeded
      }

    "return an event with all commit ids for the project which are *NOT* AWAITING_DELETION or DELETING" +
      s"where the project hasn't been synced in less than the syncFrequency" +
      s"and the project's latest event is the most recent of all ready-to-sync projects" in testDBResource.use {
        implicit cfg =>
          for {
            _ <- finder.popEvent().asserting(_ shouldBe None)

            project0           = consumerProjects.generateOne
            project0LastSynced = genLastSynced(true)
            project0Event0Date = genEventDate(olderThanAWeek = true)
            project0Event1Date = genEventDate(olderThanAWeek = false)
            project0Event2Date = genEventDate(olderThanAWeek = false)
            project0Event0 <- addEvent(project0Event0Date, project0)
            _              <- addEvent(project0Event1Date, project0, eventStatus = AwaitingDeletion)
            _              <- addEvent(project0Event2Date, project0, eventStatus = Deleting)
            _              <- upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

            _ = givenTheLastSyncedDateIsUpdated(project0)

            _ <- finder
                   .popEvent()
                   .asserting(
                     _ shouldBe GlobalCommitSyncEvent(
                       project0,
                       CommitsInfo(CommitsCount(1), CommitId(project0Event0.eventId.id.value)),
                       project0LastSynced.some
                     ).some
                   )
          } yield Succeeded
      }

    "return None " +
      "when all events are AWAITING_DELETION or DELETING but still update the last sync date of the project" in testDBResource
        .use { implicit cfg =>
          for {
            _ <- finder.popEvent().asserting(_ shouldBe None)

            project0           = consumerProjects.generateOne
            project0LastSynced = genLastSynced(true)
            project0Event0Date = genEventDate()
            project0Event1Date = genEventDate()
            project0Event2Date = genEventDate()
            _ <- addEvent(project0Event0Date, project0, eventStatus = AwaitingDeletion)
            _ <- addEvent(project0Event1Date, project0, eventStatus = AwaitingDeletion)
            _ <- addEvent(project0Event2Date, project0, eventStatus = Deleting)
            _ <- upsertCategorySyncTime(project0.id, categoryName, project0LastSynced)

            _ = givenTheLastSyncedDateIsUpdated(project0)

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield Succeeded
        }
  }

  private lazy val syncFrequency         = Duration ofDays 7
  private lazy val lastSyncedDateUpdater = mock[LastSyncedDateUpdater[IO]]
  private lazy val now                   = Instant.now()
  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventFinderImpl(lastSyncedDateUpdater, syncFrequency, () => now)
  }

  private def givenTheLastSyncedDateIsUpdated(project: Project) =
    (lastSyncedDateUpdater.run _)
      .expects(project.id, LastSyncedDate(now).some)
      .returning(Completion.Insert(1).pure[IO])

  private def genLastSynced(moreThanAWeekAgo: Boolean) =
    if (moreThanAWeekAgo)
      relativeTimestamps(moreThanAgo = syncFrequency.plus(2, HOURS)).generateAs(LastSyncedDate)
    else
      relativeTimestamps(lessThanAgo = syncFrequency.minus(2, HOURS)).generateAs(LastSyncedDate)

  private def genEventDate(olderThanAWeek: Boolean = Random.nextBoolean()) =
    if (olderThanAWeek) relativeTimestamps(moreThanAgo = Duration.ofDays(8)).generateAs(EventDate)
    else relativeTimestamps(lessThanAgo = Duration.ofDays(6)).generateAs(EventDate)

  private def genCompoundEventId(projectId: projects.GitLabId) =
    compoundEventIds.generateOne.copy(projectId = projectId)

  private def addEvent(
      eventDate:   EventDate,
      project:     Project,
      createdDate: CreatedDate = createdDates.generateOne,
      eventStatus: EventStatus =
        Gen.oneOf(EventStatus.all.filterNot(status => status == AwaitingDeletion || status == Deleting)).generateOne
  )(implicit cfg: DBConfig[EventLogDB]) =
    storeGeneratedEvent(eventStatus, eventDate, project = project, createdDate = createdDate)
}
