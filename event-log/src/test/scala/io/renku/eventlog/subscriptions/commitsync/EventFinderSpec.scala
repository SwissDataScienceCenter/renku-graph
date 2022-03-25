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

package io.renku.eventlog.subscriptions
package commitsync

import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{CreatedDate, EventDate, InMemoryEventLogDbSpec}
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus.AwaitingDeletion
import io.renku.graph.model.events.{CompoundEventId, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.time.temporal.ChronoUnit

class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return the full event for the project with the latest event date " +
      s"when the subscription_category_sync_times table does not have rows for the $categoryName " +
      "but there are events for the project" in new TestCase {

        finder.popEvent().unsafeRunSync() shouldBe None

        val event0Id          = compoundEventIds.generateOne
        val event0Date        = eventDates.generateOne
        val event0ProjectPath = projectPaths.generateOne
        addEvent(event0Id, event0Date, event0ProjectPath)

        val event1Id          = compoundEventIds.generateOne
        val event1Date        = eventDates.generateOne
        val event1ProjectPath = projectPaths.generateOne
        addEvent(event1Id, event1Date, event1ProjectPath)

        List(
          (event0Id, event0ProjectPath, event0Date),
          (event1Id, event1ProjectPath, event1Date)
        ).sortBy(_._3).reverse foreach { case (eventId, path, eventDate) =>
          finder.popEvent().unsafeRunSync() shouldBe Some(
            FullCommitSyncEvent(eventId, path, LastSyncedDate(eventDate.value))
          )
        }
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return the minimal event for the project with the latest event date " +
      s"when there are neither rows in subscription_category_sync_times table with the $categoryName " +
      "nor events for the project" in new TestCase {

        finder.popEvent().unsafeRunSync() shouldBe None

        val project1Id        = projectIds.generateOne
        val project1Path      = projectPaths.generateOne
        val project1EventDate = eventDates.generateOne
        upsertProject(project1Id, project1Path, project1EventDate)

        val project2Id        = projectIds.generateOne
        val project2Path      = projectPaths.generateOne
        val project2EventDate = eventDates.generateOne
        upsertProject(project2Id, project2Path, project2EventDate)

        List(
          (project1Id, project1Path, project1EventDate),
          (project2Id, project2Path, project2EventDate)
        ).sortBy(_._3).reverse foreach { case (projectId, path, _) =>
          finder.popEvent().unsafeRunSync() shouldBe Some(MinimalCommitSyncEvent(Project(projectId, path)))
        }
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return events for " +
      "projects with the latest event date less than a week ago " +
      "and the last sync time more than an hour ago " +
      "AND not projects with a latest event date less than a week ago " +
      "and a last sync time less than an hour ago" in new TestCase {
        val event0Id          = compoundEventIds.generateOne
        val event0ProjectPath = projectPaths.generateOne
        val event0Date        = EventDate(relativeTimestamps(lessThanAgo = Duration.ofDays(7)).generateOne)
        val event0LastSynced  = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofMinutes(61)).generateOne)
        addEvent(event0Id, event0Date, event0ProjectPath)
        upsertLastSynced(event0Id.projectId, categoryName, event0LastSynced)

        val event1Id          = compoundEventIds.generateOne
        val event1ProjectPath = projectPaths.generateOne
        val event1Date        = EventDate(relativeTimestamps(lessThanAgo = Duration.ofDays(7)).generateOne)
        val event1LastSynced  = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(59)).generateOne)
        addEvent(event1Id, event1Date, event1ProjectPath)
        upsertLastSynced(event1Id.projectId, categoryName, event1LastSynced)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          FullCommitSyncEvent(event0Id, event0ProjectPath, event0LastSynced)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return events for " +
      "projects with a latest event date more than a week ago " +
      "and a last sync time more than a day ago " +
      "AND not projects with a latest event date more than a week ago " +
      "and a last sync time less than an day ago" in new TestCase {
        val event0Id          = compoundEventIds.generateOne
        val event0ProjectPath = projectPaths.generateOne
        val event0Date        = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(7 * 24 + 1)).generateOne)
        val event0LastSynced  = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
        addEvent(event0Id, event0Date, event0ProjectPath)
        upsertLastSynced(event0Id.projectId, categoryName, event0LastSynced)

        val event1Id          = compoundEventIds.generateOne
        val event1ProjectPath = projectPaths.generateOne
        val event1Date        = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(7 * 24 + 1)).generateOne)
        val event1LastSynced  = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofHours(23)).generateOne)
        addEvent(event1Id, event1Date, event1ProjectPath)
        upsertLastSynced(event1Id.projectId, categoryName, event1LastSynced)

        finder.popEvent().unsafeRunSync() shouldBe Some(
          FullCommitSyncEvent(event0Id, event0ProjectPath, event0LastSynced)
        )
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return events for projects" +
      "which falls into the categories above " +
      "and have more than one event with the latest event date" in new TestCase {
        val commonEventDate = relativeTimestamps(moreThanAgo = Duration.ofHours(7 * 24 + 1)).generateAs(EventDate)
        val lastSynced      = relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateAs(LastSyncedDate)
        val projectId       = projectIds.generateOne
        val projectPath     = projectPaths.generateOne

        val event0Id          = compoundEventIds.generateOne.copy(projectId = projectId)
        val event0CreatedDate = createdDates.generateOne
        addEvent(event0Id, commonEventDate, projectPath, event0CreatedDate)
        upsertLastSynced(projectId, categoryName, lastSynced)

        val event1Id          = compoundEventIds.generateOne.copy(projectId = projectId)
        val event1CreatedDate = createdDates.generateOne
        addEvent(event1Id, commonEventDate, projectPath, event1CreatedDate)
        upsertLastSynced(projectId, categoryName, lastSynced)

        finder.popEvent().unsafeRunSync().map {
          case FullCommitSyncEvent(id, _, _) => id
          case _                             => fail("the test does not expect this kind of sync events")
        } shouldBe List(
          event0Id -> event0CreatedDate,
          event1Id -> event1CreatedDate
        ).sortBy(_._2).reverse.headOption.map(_._1)

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "not return events for projects" +
      "where event statuses are AWAITING_DELETION but still update the last sync date" in new TestCase {

        finder.popEvent().unsafeRunSync() shouldBe None
        val sharedProjectPath = projectPaths.generateOne

        val event0Date = EventDate(eventDates.generateOne.value.minus(1L, ChronoUnit.DAYS))
        val event0Id   = compoundEventIds.generateOne
        addEvent(event0Id, event0Date, sharedProjectPath, eventStatus = AwaitingDeletion)

        val event1Id   = compoundEventIds.generateOne.copy(projectId = event0Id.projectId)
        val event1Date = EventDate(event0Date.value.plus(1L, ChronoUnit.MINUTES))
        addEvent(event1Id, event1Date, sharedProjectPath, eventStatus = AwaitingDeletion)

        val lastSynced = relativeTimestamps(moreThanAgo = Duration.ofDays(1))
          .generateAs(LastSyncedDate)

        upsertLastSynced(event1Id.projectId, categoryName, lastSynced)

        finder.popEvent().unsafeRunSync() shouldBe None

        // This event should not be picked up as the last sync date was set to NOW()
        addEvent(compoundEventIds.generateOne.copy(projectId = event0Id.projectId),
                 eventDates.generateOne,
                 sharedProjectPath
        )

        finder.popEvent().unsafeRunSync() shouldBe None

      }
  }

  private trait TestCase {
    val finder = new EventFinderImpl(TestLabeledHistogram[SqlStatement.Name]("query_id"))
  }

  private def addEvent(eventId:     CompoundEventId,
                       eventDate:   EventDate,
                       projectPath: projects.Path,
                       createdDate: CreatedDate = createdDates.generateOne,
                       eventStatus: EventStatus =
                         Gen.oneOf(EventStatus.all.filterNot(_ == AwaitingDeletion)).generateOne
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
