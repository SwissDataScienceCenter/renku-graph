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
package membersync

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.events.{EventDate, LastSyncedDate}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import java.time.Duration

class EventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with SubscriptionProvisioning
    with should.Matchers {

  it should "return the event for the project with the latest event date " +
    s"when the subscription_category_sync_times table has no rows for the $categoryName" in testDBResource.use {
      implicit cfg =>
        for {
          _ <- finder.popEvent().asserting(_ shouldBe None)

          project0   = consumerProjects.generateOne
          eventDate0 = eventDates.generateOne
          _ <- upsertProject(project0, eventDate0)

          project1   = consumerProjects.generateOne
          eventDate1 = eventDates.generateOne
          _ <- upsertProject(project1, eventDate1)
          _ <- upsertCategorySyncTime(project1.id,
                                      commitsync.categoryName,
                                      relativeTimestamps(moreThanAgo = Duration.ofDays(30)).generateAs(LastSyncedDate)
               )

          projectsByEventDateDesc = List((project0, eventDate0), (project1, eventDate1)).sortBy(_._2).map(_._1).reverse

          _ <- finder.popEvent().asserting(_ shouldBe Some(MemberSyncEvent(projectsByEventDateDesc.head.slug)))
          _ <- finder.popEvent().asserting(_ shouldBe Some(MemberSyncEvent(projectsByEventDateDesc.tail.head.slug)))
          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield Succeeded
    }

  it should "return projects with a latest event date less than an hour ago " +
    "and a last sync time more than 5 minutes ago " +
    "AND not projects with a latest event date less than an hour ago " +
    "and a last sync time less than 5 minutes ago" in testDBResource.use { implicit cfg =>
      val project0   = consumerProjects.generateOne
      val eventDate0 = EventDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(59)).generateOne)
      val lastSynced0 =
        LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofMinutes(5).plusSeconds(1)).generateOne)
      for {
        _ <- upsertProject(project0, eventDate0)
        _ <- upsertCategorySyncTime(project0.id, categoryName, lastSynced0)

        project1   = consumerProjects.generateOne
        eventDate1 = EventDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(59)).generateOne)
        lastSynced1 =
          LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(5).minusSeconds(1)).generateOne)
        _ <- upsertProject(project1, eventDate1)
        _ <- upsertCategorySyncTime(project1.id, categoryName, lastSynced1)

        _ <- finder.popEvent().asserting(_ shouldBe Some(MemberSyncEvent(project0.slug)))
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return projects with a latest event date less than a day ago " +
    "and a last sync time more than a hour ago " +
    "but not projects with a latest event date less than a day ago " +
    "and a last sync time less than an hour ago" in testDBResource.use { implicit cfg =>
      val project0 = consumerProjects.generateOne
      val eventDate0 = EventDate(
        relativeTimestamps(lessThanAgo = Duration.ofHours(23), moreThanAgo = Duration.ofMinutes(65)).generateOne
      )
      val lastSynced0 = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofMinutes(65)).generateOne)
      for {
        _ <- upsertProject(project0, eventDate0)
        _ <- upsertCategorySyncTime(project0.id, categoryName, lastSynced0)

        project1 = consumerProjects.generateOne
        eventDate1 =
          relativeTimestamps(lessThanAgo = Duration.ofHours(23), moreThanAgo = Duration.ofMinutes(65))
            .generateAs(EventDate)
        lastSynced1 = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(55)).generateOne)
        _ <- upsertProject(project1, eventDate1)
        _ <- upsertCategorySyncTime(project1.id, categoryName, lastSynced1)

        _ <- finder.popEvent().asserting(_ shouldBe Some(MemberSyncEvent(project0.slug)))
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return projects with a latest event date more than a day ago " +
    "and a last sync time more than a day ago " +
    "but not projects with a latest event date more than a day ago " +
    "and a last sync time less than a day ago" in testDBResource.use { implicit cfg =>
      val project0    = consumerProjects.generateOne
      val eventDate0  = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
      val lastSynced0 = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
      for {
        _ <- upsertProject(project0, eventDate0)
        _ <- upsertCategorySyncTime(project0.id, categoryName, lastSynced0)

        project1    = consumerProjects.generateOne
        eventDate1  = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
        lastSynced1 = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofHours(23)).generateOne)
        _ <- upsertProject(project1, eventDate1)
        _ <- upsertCategorySyncTime(project1.id, categoryName, lastSynced1)

        _ <- finder.popEvent().asserting(_ shouldBe Some(MemberSyncEvent(project0.slug)))
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventFinderImpl[IO]
  }
}
