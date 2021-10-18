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

package io.renku.eventlog.metrics

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog._
import io.renku.eventlog.subscriptions._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonEmptyList, timestamps}
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.TestLabeledHistogram
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class StatsFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with should.Matchers {

  "countEventsByCategoryName" should {

    "return info about number of events grouped by categoryName for the memberSync category" in {
      // MEMBER_SYNC specific
      val compoundId1 = compoundEventIds.generateOne
      val eventDate1  = EventDate(generateInstant(lessThanAgo = Duration.ofMinutes(59)))
      val lastSynced1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofSeconds(61)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertLastSynced(compoundId1.projectId, membersync.categoryName, lastSynced1)

      val compoundId2 = compoundEventIds.generateOne
      val eventDate2  = EventDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      val lastSynced2 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)
      upsertLastSynced(compoundId2.projectId, membersync.categoryName, lastSynced2)

      val compoundId3 = compoundEventIds.generateOne
      val eventDate3  = EventDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
      val lastSynced3 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)
      upsertLastSynced(compoundId3.projectId, membersync.categoryName, lastSynced3)

      val compoundId4 = compoundEventIds.generateOne
      val eventDate4  = EventDate(generateInstant())
      upsertProject(compoundId4, projectPaths.generateOne, eventDate4)

      // MEMBER_SYNC should not see this one
      val compoundId5 = compoundEventIds.generateOne
      val eventDate5  = EventDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
      val lastSynced5 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      upsertProject(compoundId5, projectPaths.generateOne, eventDate5)
      upsertLastSynced(compoundId5.projectId, membersync.categoryName, lastSynced5)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName       -> 4L,
        commitsync.categoryName       -> 5L,
        globalcommitsync.categoryName -> 5L
      )
    }

    "return info about number of events grouped by categoryName for the commitSync category" in {

      // COMMIT_SYNC specific
      val compoundId1   = compoundEventIds.generateOne
      val eventDate1    = EventDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      val lastSyncDate1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertLastSynced(compoundId1.projectId, commitsync.categoryName, lastSyncDate1)

      val compoundId2   = compoundEventIds.generateOne
      val eventDate2    = EventDate(generateInstant(moreThanAgo = Duration.ofHours(7 * 24 + 1)))
      val lastSyncDate2 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(25)))
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)
      upsertLastSynced(compoundId2.projectId, commitsync.categoryName, lastSyncDate2)

      val compoundId3 = compoundEventIds.generateOne
      val eventDate3  = EventDate(generateInstant())
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)

      // COMMIT_SYNC should not see this one
      val compoundId4   = compoundEventIds.generateOne
      val eventDate4    = EventDate(generateInstant(moreThanAgo = Duration.ofHours(7 * 24 + 1)))
      val lastSyncDate4 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
      upsertProject(compoundId4, projectPaths.generateOne, eventDate4)
      upsertLastSynced(compoundId4.projectId, commitsync.categoryName, lastSyncDate4)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName       -> 4L,
        commitsync.categoryName       -> 3L,
        globalcommitsync.categoryName -> 4L
      )
    }

    "return info about number of events grouped by categoryName for the globalCommitSync category" in {

      // GLOBAL_COMMIT_SYNC specific
      val compoundId1   = compoundEventIds.generateOne
      val eventDate1    = EventDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      val lastSyncDate1 = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      upsertProject(compoundId1, projectPaths.generateOne, eventDate1)
      upsertLastSynced(compoundId1.projectId, globalcommitsync.categoryName, lastSyncDate1)

      val compoundId2 = compoundEventIds.generateOne
      val eventDate2  = EventDate(generateInstant())
      upsertProject(compoundId2, projectPaths.generateOne, eventDate2)

      // GLOBAL_COMMIT_SYNC should not see this one
      val compoundId3   = compoundEventIds.generateOne
      val eventDate3    = EventDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
      val lastSyncDate3 = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
      upsertProject(compoundId3, projectPaths.generateOne, eventDate3)
      upsertLastSynced(compoundId3.projectId, globalcommitsync.categoryName, lastSyncDate3)

      stats.countEventsByCategoryName().unsafeRunSync() shouldBe Map(
        membersync.categoryName       -> 3L,
        commitsync.categoryName       -> 3L,
        globalcommitsync.categoryName -> 2L
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
  private lazy val stats            = new StatsFinderImpl(sessionResource, queriesExecTimes)

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
