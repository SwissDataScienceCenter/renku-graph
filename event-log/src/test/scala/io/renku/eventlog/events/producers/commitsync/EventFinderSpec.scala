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

package io.renku.eventlog.events.producers.commitsync

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.producers.SubscriptionProvisioning
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.relativeTimestamps
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus.AwaitingDeletion
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.Duration
import java.time.temporal.ChronoUnit

class EventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with SubscriptionProvisioning
    with AsyncMockFactory
    with should.Matchers {

  it should "return the full event for the project with the latest event date " +
    s"when the subscription_category_sync_times table does not have rows for the $categoryName " +
    "but there are events for the project" in testDBResource.use { implicit cfg =>
      for {
        _ <- finder.popEvent().asserting(_ shouldBe None)

        event0Id          = compoundEventIds.generateOne
        event0Date        = eventDates.generateOne
        event0ProjectSlug = projectSlugs.generateOne
        _ <- addEvent(event0Id, event0Date, event0ProjectSlug)

        event1Id          = compoundEventIds.generateOne
        event1Date        = eventDates.generateOne
        event1ProjectSlug = projectSlugs.generateOne
        _ <- addEvent(event1Id, event1Date, event1ProjectSlug)

        _ <- List(
               FullCommitSyncEvent(event0Id, event0ProjectSlug, LastSyncedDate(event0Date.value)),
               FullCommitSyncEvent(event1Id, event1ProjectSlug, LastSyncedDate(event1Date.value))
             ).sortBy(_.lastSyncedDate)
               .reverse
               .map(event => finder.popEvent().asserting(_ shouldBe Some(event)))
               .sequence
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return the minimal event for the project with the latest event date " +
    s"when there are neither rows in subscription_category_sync_times table with the $categoryName " +
    "nor events for the project" in testDBResource.use { implicit cfg =>
      for {
        _ <- finder.popEvent().asserting(_ shouldBe None)

        project1Id        = projectIds.generateOne
        project1Slug      = projectSlugs.generateOne
        project1EventDate = eventDates.generateOne
        _ <- upsertProject(project1Id, project1Slug, project1EventDate)

        project2Id        = projectIds.generateOne
        project2Slug      = projectSlugs.generateOne
        project2EventDate = eventDates.generateOne
        _ <- upsertProject(project2Id, project2Slug, project2EventDate)

        _ <-
          List(
            (Project(project1Id, project1Slug), project1EventDate),
            (Project(project2Id, project2Slug), project2EventDate)
          ).sortBy(_._2)
            .reverse
            .map { case (project, _) => finder.popEvent().asserting(_ shouldBe Some(MinimalCommitSyncEvent(project))) }
            .sequence
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return events for " +
    "projects with the latest event date less than a week ago " +
    "and the last sync time more than an hour ago " +
    "AND not projects with a latest event date less than a week ago " +
    "and a last sync time less than an hour ago" in testDBResource.use { implicit cfg =>
      for {
        _ <- finder.popEvent().asserting(_ shouldBe None)

        event0Id          = compoundEventIds.generateOne
        event0ProjectSlug = projectSlugs.generateOne
        event0Date        = EventDate(relativeTimestamps(lessThanAgo = Duration.ofDays(7)).generateOne)
        event0LastSynced  = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofMinutes(61)).generateOne)
        _ <- addEvent(event0Id, event0Date, event0ProjectSlug)
        _ <- upsertCategorySyncTime(event0Id.projectId, categoryName, event0LastSynced)

        event1Id          = compoundEventIds.generateOne
        event1ProjectSlug = projectSlugs.generateOne
        event1Date        = EventDate(relativeTimestamps(lessThanAgo = Duration.ofDays(7)).generateOne)
        event1LastSynced  = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(59)).generateOne)
        _ <- addEvent(event1Id, event1Date, event1ProjectSlug)
        _ <- upsertCategorySyncTime(event1Id.projectId, categoryName, event1LastSynced)

        _ <- finder
               .popEvent()
               .asserting(_ shouldBe Some(FullCommitSyncEvent(event0Id, event0ProjectSlug, event0LastSynced)))
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return events for " +
    "projects with a latest event date more than a week ago " +
    "and a last sync time more than a day ago " +
    "AND not projects with a latest event date more than a week ago " +
    "and a last sync time less than an day ago" in testDBResource.use { implicit cfg =>
      for {
        _ <- finder.popEvent().asserting(_ shouldBe None)

        event0Id          = compoundEventIds.generateOne
        event0ProjectSlug = projectSlugs.generateOne
        event0Date        = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(7 * 24 + 1)).generateOne)
        event0LastSynced  = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
        _ <- addEvent(event0Id, event0Date, event0ProjectSlug)
        _ <- upsertCategorySyncTime(event0Id.projectId, categoryName, event0LastSynced)

        event1Id          = compoundEventIds.generateOne
        event1ProjectSlug = projectSlugs.generateOne
        event1Date        = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(7 * 24 + 1)).generateOne)
        event1LastSynced  = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofHours(23)).generateOne)
        _ <- addEvent(event1Id, event1Date, event1ProjectSlug)
        _ <- upsertCategorySyncTime(event1Id.projectId, categoryName, event1LastSynced)

        _ <- finder
               .popEvent()
               .asserting(_ shouldBe Some(FullCommitSyncEvent(event0Id, event0ProjectSlug, event0LastSynced)))
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return events for projects" +
    "which falls into the categories above " +
    "and have more than one event with the latest event date" in testDBResource.use { implicit cfg =>
      for {
        _ <- finder.popEvent().asserting(_ shouldBe None)

        commonEventDate = relativeTimestamps(moreThanAgo = Duration.ofHours(7 * 24 + 1)).generateAs(EventDate)
        lastSynced      = relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateAs(LastSyncedDate)
        projectId       = projectIds.generateOne
        projectSlug     = projectSlugs.generateOne

        event0Id          = compoundEventIds.generateOne.copy(projectId = projectId)
        event0CreatedDate = createdDates.generateOne
        _ <- addEvent(event0Id, commonEventDate, projectSlug, event0CreatedDate)
        _ <- upsertCategorySyncTime(projectId, categoryName, lastSynced)

        event1Id          = compoundEventIds.generateOne.copy(projectId = projectId)
        event1CreatedDate = createdDates.generateOne
        _ <- addEvent(event1Id, commonEventDate, projectSlug, event1CreatedDate)
        _ <- upsertCategorySyncTime(projectId, categoryName, lastSynced)

        _ <- finder.popEvent().asserting {
               _.map {
                 case FullCommitSyncEvent(id, _, _) => id
                 case _                             => fail("the test does not expect this kind of sync events")
               } shouldBe List(
                 event0Id -> event0CreatedDate,
                 event1Id -> event1CreatedDate
               ).sortBy(_._2).reverse.headOption.map(_._1)
             }
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "not return events for projects" +
    "where event statuses are AWAITING_DELETION but still update the last sync date" in testDBResource.use {
      implicit cfg =>
        for {
          _ <- finder.popEvent().asserting(_ shouldBe None)

          sharedProjectSlug = projectSlugs.generateOne

          event0Date = EventDate(eventDates.generateOne.value.minus(1L, ChronoUnit.DAYS))
          event0Id   = compoundEventIds.generateOne
          _ <- addEvent(event0Id, event0Date, sharedProjectSlug, eventStatus = AwaitingDeletion)

          event1Id   = compoundEventIds.generateOne.copy(projectId = event0Id.projectId)
          event1Date = EventDate(event0Date.value.plus(1L, ChronoUnit.MINUTES))
          _ <- addEvent(event1Id, event1Date, sharedProjectSlug, eventStatus = AwaitingDeletion)

          lastSynced = relativeTimestamps(moreThanAgo = Duration.ofDays(1)).generateAs(LastSyncedDate)
          _ <- upsertCategorySyncTime(event1Id.projectId, categoryName, lastSynced)

          _ <- finder.popEvent().asserting(_ shouldBe None)

          // This event should not be picked up as the last sync date was set to NOW()
          _ <- addEvent(compoundEventIds.generateOne.copy(projectId = event0Id.projectId),
                        eventDates.generateOne,
                        sharedProjectSlug
               )

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield Succeeded
    }

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val queriesExecTimes: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventFinderImpl[IO]
  }

  private def addEvent(
      eventId:     CompoundEventId,
      eventDate:   EventDate,
      projectSlug: projects.Slug,
      createdDate: CreatedDate = createdDates.generateOne,
      eventStatus: EventStatus = Gen.oneOf(EventStatus.all.filterNot(_ == AwaitingDeletion)).generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    storeEvent(eventId,
               eventStatus,
               executionDates.generateOne,
               eventDate,
               eventBodies.generateOne,
               createdDate,
               projectSlug = projectSlug
    )
}
