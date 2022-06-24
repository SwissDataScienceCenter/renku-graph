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

package io.renku.eventlog.metrics

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.events.producers._
import io.renku.events.CategoryName
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonEmptyList, nonEmptyStrings, positiveInts, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant, OffsetDateTime}

class StatsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with CleanUpEventsProvisioning
    with should.Matchers {

  "countEventsByCategoryName" should {

    "return info about number of events grouped by categoryName for the memberSync category" in {

      val compoundId1 = compoundEventIds.generateOne
      val eventDate1  = EventDate(generateInstant(lessThanAgo = Duration.ofMinutes(59)))
      val lastSynced1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(6)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertCategorySyncTime(compoundId1.projectId, membersync.categoryName, lastSynced1)

      val compoundId2 = compoundEventIds.generateOne
      val eventDate2  = EventDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      val lastSynced2 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)
      upsertCategorySyncTime(compoundId2.projectId, membersync.categoryName, lastSynced2)

      val compoundId3 = compoundEventIds.generateOne
      val eventDate3  = EventDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
      val lastSynced3 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)
      upsertCategorySyncTime(compoundId3.projectId, membersync.categoryName, lastSynced3)

      val compoundId4 = compoundEventIds.generateOne
      val eventDate4  = EventDate(generateInstant())
      upsertProject(compoundId4, projectPaths.generateOne, eventDate4)

      // MEMBER_SYNC should not see this one
      val compoundId5 = compoundEventIds.generateOne
      val eventDate5  = EventDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
      val lastSynced5 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      upsertProject(compoundId5, projectPaths.generateOne, eventDate5)
      upsertCategorySyncTime(compoundId5.projectId, membersync.categoryName, lastSynced5)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName        -> 4L,
        commitsync.categoryName        -> 5L,
        globalcommitsync.categoryName  -> 5L,
        projectsync.categoryName       -> 5L,
        minprojectinfo.categoryName    -> 5L,
        CategoryName("CLEAN_UP_EVENT") -> 0L
      )
    }

    "return info about number of events grouped by categoryName for the commitSync category" in {

      val compoundId1   = compoundEventIds.generateOne
      val eventDate1    = EventDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      val lastSyncDate1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertCategorySyncTime(compoundId1.projectId, commitsync.categoryName, lastSyncDate1)

      val compoundId2   = compoundEventIds.generateOne
      val eventDate2    = EventDate(generateInstant(moreThanAgo = Duration.ofHours(7 * 24 + 1)))
      val lastSyncDate2 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(25)))
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)
      upsertCategorySyncTime(compoundId2.projectId, commitsync.categoryName, lastSyncDate2)

      val compoundId3 = compoundEventIds.generateOne
      val eventDate3  = EventDate(generateInstant())
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)

      // COMMIT_SYNC should not see this one
      val compoundId4   = compoundEventIds.generateOne
      val eventDate4    = EventDate(generateInstant(moreThanAgo = Duration.ofHours(7 * 24 + 1)))
      val lastSyncDate4 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      upsertProject(compoundId4, projectPaths.generateOne, eventDate4)
      upsertCategorySyncTime(compoundId4.projectId, commitsync.categoryName, lastSyncDate4)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName        -> 4L,
        commitsync.categoryName        -> 3L,
        globalcommitsync.categoryName  -> 4L,
        projectsync.categoryName       -> 4L,
        minprojectinfo.categoryName    -> 4L,
        CategoryName("CLEAN_UP_EVENT") -> 0L
      )
    }

    "return info about number of events grouped by categoryName for the globalCommitSync category" in {

      val compoundId1   = compoundEventIds.generateOne
      val eventDate1    = EventDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      val lastSyncDate1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertCategorySyncTime(compoundId1.projectId, globalcommitsync.categoryName, lastSyncDate1)

      val compoundId2 = compoundEventIds.generateOne
      val eventDate2  = EventDate(generateInstant())
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)

      // GLOBAL_COMMIT_SYNC should not see this one
      val compoundId3   = compoundEventIds.generateOne
      val eventDate3    = EventDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      val lastSyncDate3 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)
      upsertCategorySyncTime(compoundId3.projectId, globalcommitsync.categoryName, lastSyncDate3)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName        -> 3L,
        commitsync.categoryName        -> 3L,
        globalcommitsync.categoryName  -> 2L,
        projectsync.categoryName       -> 3L,
        minprojectinfo.categoryName    -> 3L,
        CategoryName("CLEAN_UP_EVENT") -> 0L
      )
    }

    "return info about number of events grouped by categoryName for the projectSync category" in {

      val compoundId1   = compoundEventIds.generateOne
      val eventDate1    = EventDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      val lastSyncDate1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertCategorySyncTime(compoundId1.projectId, globalcommitsync.categoryName, lastSyncDate1)

      val compoundId2 = compoundEventIds.generateOne
      val eventDate2  = EventDate(generateInstant())
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)

      // PROJECT_SYNC should not see this one
      val compoundId3   = compoundEventIds.generateOne
      val eventDate3    = EventDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      val lastSyncDate3 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)
      upsertCategorySyncTime(compoundId3.projectId, projectsync.categoryName, lastSyncDate3)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName        -> 3L,
        commitsync.categoryName        -> 3L,
        globalcommitsync.categoryName  -> 3L,
        projectsync.categoryName       -> 2L,
        minprojectinfo.categoryName    -> 3L,
        CategoryName("CLEAN_UP_EVENT") -> 0L
      )
    }

    "return info about number of events grouped by categoryName for the minProjectInfo category" in {

      val compoundId1   = compoundEventIds.generateOne
      val eventDate1    = EventDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      val lastSyncDate1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertCategorySyncTime(compoundId1.projectId, globalcommitsync.categoryName, lastSyncDate1)

      val compoundId2 = compoundEventIds.generateOne
      val eventDate2  = EventDate(generateInstant())
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)

      // ADD_MIN_PROJECT_INFO should not see this one
      val compoundId3    = compoundEventIds.generateOne
      val lastEventDate3 = eventDates.generateOne
      upsertProject(compoundId3, projectPaths.generateOne, lastEventDate3)
      upsertCategorySyncTime(compoundId3.projectId,
                             minprojectinfo.categoryName,
                             timestampsNotInTheFuture(lastEventDate3.value).generateAs(LastSyncedDate)
      )

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName        -> 3L,
        commitsync.categoryName        -> 3L,
        globalcommitsync.categoryName  -> 3L,
        projectsync.categoryName       -> 3L,
        minprojectinfo.categoryName    -> 2L,
        CategoryName("CLEAN_UP_EVENT") -> 0L
      )
    }

    "return info about number of events grouped by event_type in the status_change_events_queue" in {

      val type1 = nonEmptyStrings().generateOne
      insertEventIntoEventsQueue(type1, jsons.generateOne)
      insertEventIntoEventsQueue(type1, jsons.generateOne)

      val type2 = nonEmptyStrings().generateOne
      insertEventIntoEventsQueue(type2, jsons.generateOne)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        CategoryName(type1)            -> 2L,
        CategoryName(type2)            -> 1L,
        membersync.categoryName        -> 0L,
        commitsync.categoryName        -> 0L,
        globalcommitsync.categoryName  -> 0L,
        projectsync.categoryName       -> 0L,
        minprojectinfo.categoryName    -> 0L,
        CategoryName("CLEAN_UP_EVENT") -> 0L
      )
    }

    "return info about number of events in the clean_up_events_queue" in {

      val eventsCount = positiveInts(max = 20).generateOne.value
      1 to eventsCount foreach { _ =>
        insertCleanUpEvent(projectIds.generateOne, projectPaths.generateOne, OffsetDateTime.now())
      }

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        CategoryName("CLEAN_UP_EVENT") -> eventsCount.toLong,
        membersync.categoryName        -> 0L,
        commitsync.categoryName        -> 0L,
        globalcommitsync.categoryName  -> 0L,
        projectsync.categoryName       -> 0L,
        minprojectinfo.categoryName    -> 0L
      )
    }
  }

  "statuses" should {

    "return info about number of events grouped by status" in {
      val statuses = eventStatuses.generateNonEmptyList().toList

      statuses foreach store

      stats.statuses().unsafeRunSync() shouldBe Map(
        New                                 -> statuses.count(_ == New),
        GeneratingTriples                   -> statuses.count(_ == GeneratingTriples),
        TriplesGenerated                    -> statuses.count(_ == TriplesGenerated),
        TransformingTriples                 -> statuses.count(_ == TransformingTriples),
        TriplesStore                        -> statuses.count(_ == TriplesStore),
        Skipped                             -> statuses.count(_ == Skipped),
        GenerationRecoverableFailure        -> statuses.count(_ == GenerationRecoverableFailure),
        GenerationNonRecoverableFailure     -> statuses.count(_ == GenerationNonRecoverableFailure),
        TransformationRecoverableFailure    -> statuses.count(_ == TransformationRecoverableFailure),
        TransformationNonRecoverableFailure -> statuses.count(_ == TransformationNonRecoverableFailure),
        Deleting                            -> statuses.count(_ == Deleting),
        AwaitingDeletion                    -> statuses.count(_ == AwaitingDeletion)
      )

      queriesExecTimes.verifyExecutionTimeMeasured("statuses count")
    }
  }

  "countEvents" should {

    "return info about number of events with the given status in the queue grouped by projects" in {
      val events = generateEventsFor(
        projectPaths.generateNonEmptyList(minElements = 2, maxElements = 8).toList
      )

      events foreach store

      stats.countEvents(Set(New, GenerationRecoverableFailure)).unsafeRunSync() shouldBe events
        .groupBy(_._1)
        .map { case (projectPath, sameProjectGroup) =>
          projectPath -> sameProjectGroup.count { case (_, _, status, _) =>
            Set(New, GenerationRecoverableFailure) contains status
          }
        }
        .filter { case (_, count) => count > 0 }

      queriesExecTimes.verifyExecutionTimeMeasured("projects events count")
    }

    "return info about number of events with the given status in the queue from the first given number of projects" in {
      val projectPathsList = nonEmptyList(projectPaths, minElements = 3, maxElements = 8).generateOne
      val events           = generateEventsFor(projectPathsList.toList)

      events foreach store

      val limit: Int Refined Positive = 2

      stats.countEvents(Set(New, GenerationRecoverableFailure), Some(limit)).unsafeRunSync() shouldBe events
        .groupBy(_._1)
        .toSeq
        .sortBy { case (_, sameProjectGroup) =>
          val (_, _, _, maxEventDate) = sameProjectGroup.maxBy { case (_, _, _, eventDate) => eventDate }
          maxEventDate
        }
        .reverse
        .map { case (projectPath, sameProjectGroup) =>
          projectPath -> sameProjectGroup.count { case (_, _, status, _) =>
            Set(New, GenerationRecoverableFailure) contains status
          }
        }
        .filter { case (_, count) => count > 0 }
        .take(limit.value)
        .toMap

      queriesExecTimes.verifyExecutionTimeMeasured("projects events count")
    }
  }

  private lazy val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
  private lazy val stats            = new StatsFinderImpl(queriesExecTimes)

  private def store: ((Path, EventId, EventStatus, EventDate)) => Unit = {
    case (projectPath, eventId, status, eventDate) =>
      storeEvent(
        CompoundEventId(eventId, Id(Math.abs(projectPath.value.hashCode))),
        status,
        executionDates.generateOne,
        eventDate,
        eventBodies.generateOne,
        projectPath = projectPath
      )
  }

  private def store(status: EventStatus): Unit =
    storeEvent(compoundEventIds.generateOne,
               status,
               executionDates.generateOne,
               eventDates.generateOne,
               eventBodies.generateOne
    )

  private def generateEventsFor(projectPaths: List[Path]) =
    projectPaths flatMap { projectPath =>
      nonEmptyList(eventData, maxElements = 20).generateOne.toList.map { case (commitId, status, eventDate) =>
        (projectPath, commitId, status, eventDate)
      }
    }

  private val eventData = for {
    eventId   <- eventIds
    status    <- eventStatuses
    eventDate <- eventDates
  } yield (eventId, status, eventDate)

  private def generateInstant(lessThanAgo: Duration = Duration.ofDays(365 * 5), moreThanAgo: Duration = Duration.ZERO) =
    timestamps(min = Instant.now.minus(lessThanAgo), max = Instant.now.minus(moreThanAgo)).generateOne
}
