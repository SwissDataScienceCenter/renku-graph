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

package io.renku.eventlog.metrics

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.producers._
import io.renku.eventlog.{CleanUpEventsProvisioning, EventLogDB, EventLogPostgresSpec}
import io.renku.events.CategoryName
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonEmptyStrings, positiveInts, timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}

class StatsFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with CleanUpEventsProvisioning
    with should.Matchers {

  "countEventsByCategoryName" should {

    "return info about number of events grouped by categoryName for the memberSync category" in testDBResource.use {
      implicit cfg =>
        for {
          project1 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project1, EventDate(generateInstant(lessThanAgo = Duration.ofMinutes(59))))
          _ <- upsertCategorySyncTime(project1.id,
                                      membersync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(6)))
               )

          project2 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project2, EventDate(generateInstant(lessThanAgo = Duration.ofHours(23))))
          _ <- upsertCategorySyncTime(project2.id,
                                      membersync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
               )

          project3 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project3, EventDate(generateInstant(moreThanAgo = Duration.ofHours(25))))
          _ <- upsertCategorySyncTime(project3.id,
                                      membersync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
               )

          project4 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project4, EventDate(generateInstant()))

          // MEMBER_SYNC should not see this one
          project5 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project5, EventDate(generateInstant(moreThanAgo = Duration.ofHours(25))))
          _ <- upsertCategorySyncTime(project5.id,
                                      membersync.categoryName,
                                      LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
               )

          _ <- stats.countEventsByCategoryName().asserting {
                 _ shouldBe Map(
                   membersync.categoryName        -> 4L,
                   commitsync.categoryName        -> 5L,
                   globalcommitsync.categoryName  -> 5L,
                   projectsync.categoryName       -> 5L,
                   minprojectinfo.categoryName    -> 5L,
                   CategoryName("CLEAN_UP_EVENT") -> 0L
                 )
               }
        } yield Succeeded
    }

    "return info about number of events grouped by categoryName for the commitSync category" in testDBResource.use {
      implicit cfg =>
        for {
          project1 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project1, EventDate(generateInstant(lessThanAgo = Duration.ofDays(7))))
          _ <- upsertCategorySyncTime(project1.id,
                                      commitsync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
               )

          project2 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project2, EventDate(generateInstant(moreThanAgo = Duration.ofHours(7 * 24 + 1))))
          _ <- upsertCategorySyncTime(project2.id,
                                      commitsync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(25)))
               )

          _ <- upsertProject(consumerProjects.generateOne, EventDate(generateInstant()))

          // COMMIT_SYNC should not see this one
          project4 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project4, EventDate(generateInstant(moreThanAgo = Duration.ofHours(7 * 24 + 1))))
          _ <- upsertCategorySyncTime(project4.id,
                                      commitsync.categoryName,
                                      LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
               )

          _ <- stats.countEventsByCategoryName().asserting {
                 _ shouldBe Map(
                   membersync.categoryName        -> 4L,
                   commitsync.categoryName        -> 3L,
                   globalcommitsync.categoryName  -> 4L,
                   projectsync.categoryName       -> 4L,
                   minprojectinfo.categoryName    -> 4L,
                   CategoryName("CLEAN_UP_EVENT") -> 0L
                 )
               }
        } yield Succeeded
    }

    "return info about number of events grouped by categoryName for the globalCommitSync category" in testDBResource
      .use { implicit cfg =>
        for {
          project1 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project1, EventDate(generateInstant(lessThanAgo = Duration.ofDays(7))))
          _ <- upsertCategorySyncTime(project1.id,
                                      globalcommitsync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
               )

          _ <- upsertProject(consumerProjects.generateOne, EventDate(generateInstant()))

          // GLOBAL_COMMIT_SYNC should not see this one
          project3 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project3, EventDate(generateInstant(moreThanAgo = Duration.ofDays(7))))
          _ <- upsertCategorySyncTime(project3.id,
                                      globalcommitsync.categoryName,
                                      LastSyncedDate(generateInstant(lessThanAgo = Duration.ofDays(7)))
               )

          _ <- stats.countEventsByCategoryName().asserting {
                 _ shouldBe Map(
                   membersync.categoryName        -> 3L,
                   commitsync.categoryName        -> 3L,
                   globalcommitsync.categoryName  -> 2L,
                   projectsync.categoryName       -> 3L,
                   minprojectinfo.categoryName    -> 3L,
                   CategoryName("CLEAN_UP_EVENT") -> 0L
                 )
               }
        } yield Succeeded
      }

    "return info about number of events grouped by categoryName for the projectSync category" in testDBResource.use {
      implicit cfg =>
        for {
          project1 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project1, EventDate(generateInstant(lessThanAgo = Duration.ofDays(7))))
          _ <- upsertCategorySyncTime(project1.id,
                                      globalcommitsync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
               )

          _ <- upsertProject(consumerProjects.generateOne, EventDate(generateInstant()))

          // PROJECT_SYNC should not see this one
          project3 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project3, EventDate(generateInstant(moreThanAgo = Duration.ofDays(7))))
          _ <- upsertCategorySyncTime(project3.id,
                                      projectsync.categoryName,
                                      LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
               )

          _ <- stats.countEventsByCategoryName().asserting {
                 _ shouldBe Map(
                   membersync.categoryName        -> 3L,
                   commitsync.categoryName        -> 3L,
                   globalcommitsync.categoryName  -> 3L,
                   projectsync.categoryName       -> 2L,
                   minprojectinfo.categoryName    -> 3L,
                   CategoryName("CLEAN_UP_EVENT") -> 0L
                 )
               }
        } yield Succeeded
    }

    "return info about number of events grouped by categoryName for the minProjectInfo category" in testDBResource.use {
      implicit cfg =>
        for {
          project1 <- consumerProjects.generateOne.pure[IO]
          _        <- upsertProject(project1, EventDate(generateInstant(lessThanAgo = Duration.ofDays(7))))
          _ <- upsertCategorySyncTime(project1.id,
                                      globalcommitsync.categoryName,
                                      LastSyncedDate(generateInstant(moreThanAgo = Duration.ofDays(7)))
               )

          _ <- upsertProject(consumerProjects.generateOne, EventDate(generateInstant()))

          // ADD_MIN_PROJECT_INFO should not see this one
          project3 <- consumerProjects.generateOne.pure[IO]
          lastEventDate3 = eventDates.generateOne
          _ <- upsertProject(project3, lastEventDate3)
          _ <- upsertCategorySyncTime(project3.id,
                                      minprojectinfo.categoryName,
                                      timestampsNotInTheFuture(lastEventDate3.value).generateAs(LastSyncedDate)
               )

          _ <- stats.countEventsByCategoryName().asserting {
                 _ shouldBe Map(
                   membersync.categoryName        -> 3L,
                   commitsync.categoryName        -> 3L,
                   globalcommitsync.categoryName  -> 3L,
                   projectsync.categoryName       -> 3L,
                   minprojectinfo.categoryName    -> 2L,
                   CategoryName("CLEAN_UP_EVENT") -> 0L
                 )
               }
        } yield Succeeded
    }

    "return info about number of events grouped by event_type in the status_change_events_queue" in testDBResource.use {
      implicit cfg =>
        for {
          type1 <- nonEmptyStrings().generateOne.pure[IO]
          _     <- insertEventIntoEventsQueue(type1, jsons.generateOne)
          _     <- insertEventIntoEventsQueue(type1, jsons.generateOne)

          type2 = nonEmptyStrings().generateOne
          _ <- insertEventIntoEventsQueue(type2, jsons.generateOne)

          _ <- stats.countEventsByCategoryName().asserting {
                 _ shouldBe Map(
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
        } yield Succeeded
    }

    "return info about number of events in the clean_up_events_queue" in testDBResource.use { implicit cfg =>
      val eventsCount = positiveInts(max = 20).generateOne.value
      for {
        _ <- (1 to eventsCount).toList.traverse_(_ => insertCleanUpEvent(consumerProjects.generateOne))

        _ <- stats.countEventsByCategoryName().asserting {
               _ shouldBe Map(
                 CategoryName("CLEAN_UP_EVENT") -> eventsCount.toLong,
                 membersync.categoryName        -> 0L,
                 commitsync.categoryName        -> 0L,
                 globalcommitsync.categoryName  -> 0L,
                 projectsync.categoryName       -> 0L,
                 minprojectinfo.categoryName    -> 0L
               )
             }
      } yield Succeeded
    }
  }

  "statuses" should {

    "return info about number of events grouped by status" in testDBResource.use { implicit cfg =>
      val statuses = eventStatuses.generateNonEmptyList().toList

      statuses.traverse_(store) >>
        stats.statuses().asserting {
          _ shouldBe Map(
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
        }
    }
  }

  "countEvents" should {

    "return info about number of events with the given status in the queue grouped by projects" in testDBResource.use {
      implicit cfg =>
        for {
          events <- generateEventsFor(consumerProjects.generateNonEmptyList(min = 2, max = 8).toList)

          _ <- stats.countEvents(Set(New, GenerationRecoverableFailure)).asserting {
                 _ shouldBe events
                   .groupBy(_.project.slug)
                   .map { case (projectSlug, sameProjectGroup) =>
                     projectSlug -> sameProjectGroup.count { ev =>
                       Set(New, GenerationRecoverableFailure) contains ev.status
                     }
                   }
                   .filter { case (_, count) => count > 0 }
               }
        } yield Succeeded
    }

    "return info about number of events with the given status in the queue from the first given number of projects" in testDBResource
      .use { implicit cfg =>
        val limit: Int Refined Positive = 2
        for {
          events <- generateEventsFor(consumerProjects.generateList(min = 3, max = 8))

          _ <- stats.countEvents(Set(New, GenerationRecoverableFailure), Some(limit)).asserting {
                 _ shouldBe events
                   .groupBy(_.project.slug)
                   .toSeq
                   .sortBy { case (_, sameProjectGroup) => sameProjectGroup.maxBy(_.eventDate).eventDate }
                   .reverse
                   .map { case (projectSlug, sameProjectGroup) =>
                     projectSlug -> sameProjectGroup.count { ev =>
                       Set(New, GenerationRecoverableFailure) contains ev.status
                     }
                   }
                   .filter { case (_, count) => count > 0 }
                   .take(limit.value)
                   .toMap
               }
        } yield Succeeded
      }
  }

  private def stats(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new StatsFinderImpl[IO]
  }

  private def store(status: EventStatus)(implicit cfg: DBConfig[EventLogDB]): IO[GeneratedEvent] =
    storeGeneratedEvent(status, eventDates.generateOne, consumerProjects.generateOne)

  private def generateEventsFor(projects: List[Project])(implicit cfg: DBConfig[EventLogDB]) =
    projects
      .map(storeGeneratedEvent(eventStatuses.generateOne, eventDates.generateOne, _))
      .sequence

  private def generateInstant(lessThanAgo: Duration = Duration.ofDays(365 * 5), moreThanAgo: Duration = Duration.ZERO) =
    timestamps(min = Instant.now.minus(lessThanAgo), max = Instant.now.minus(moreThanAgo)).generateOne
}
