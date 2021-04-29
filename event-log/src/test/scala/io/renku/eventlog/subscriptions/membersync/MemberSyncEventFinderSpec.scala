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

package io.renku.eventlog.subscriptions
package membersync

import ch.datascience.db.SqlStatement
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.LastSyncedDate
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.subscriptions.SubscriptionDataProvisioning
import io.renku.eventlog.{EventDate, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class MemberSyncEventFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return the event for the project with the latest event date " +
      s"when the subscription_category_sync_times table has no rows for the $categoryName" in new TestCase {

        finder.popEvent().unsafeRunSync() shouldBe None

        val projectPath0 = projectPaths.generateOne
        val eventDate0   = eventDates.generateOne
        upsertProject(compoundEventIds.generateOne, projectPath0, eventDate0)

        val eventId1     = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = eventDates.generateOne
        upsertProject(eventId1, projectPath1, eventDate1)
        upsertLastSynced(eventId1.projectId,
                         commitsync.categoryName,
                         relativeTimestamps(moreThanAgo = Duration.ofDays(30)).generateAs(LastSyncedDate)
        )

        val projectPathsByDateDecreasing = List(
          (projectPath0, eventDate0),
          (projectPath1, eventDate1)
        ).sortBy(_._2).map(_._1).reverse
        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPathsByDateDecreasing.head))
        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPathsByDateDecreasing.tail.head))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return projects with a latest event date less than an hour ago " +
      "and a last sync time more than a minute ago " +
      "AND not projects with a latest event date less than an hour ago " +
      "and a last sync time less than a minute ago" in new TestCase {
        val compoundId0  = compoundEventIds.generateOne
        val projectPath0 = projectPaths.generateOne
        val eventDate0   = EventDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(59)).generateOne)
        val lastSynced0  = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofSeconds(62)).generateOne)
        upsertProject(compoundId0, projectPath0, eventDate0)
        upsertLastSynced(compoundId0.projectId, categoryName, lastSynced0)

        val compoundId1  = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = EventDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(59)).generateOne)
        val lastSynced1  = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofSeconds(58)).generateOne)
        upsertProject(compoundId1, projectPath1, eventDate1)
        upsertLastSynced(compoundId1.projectId, categoryName, lastSynced1)

        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPath0))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return projects with a latest event date less than a day ago " +
      "and a last sync time more than a hour ago " +
      "but not projects with a latest event date less than a day ago " +
      "and a last sync time less than an hour ago" in new TestCase {
        val compoundId0  = compoundEventIds.generateOne
        val projectPath0 = projectPaths.generateOne
        val eventDate0 = EventDate(
          relativeTimestamps(lessThanAgo = Duration.ofHours(23), moreThanAgo = Duration.ofMinutes(65)).generateOne
        )
        val lastSynced0 = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofMinutes(65)).generateOne)
        upsertProject(compoundId0, projectPath0, eventDate0)
        upsertLastSynced(compoundId0.projectId, categoryName, lastSynced0)

        val compoundId1  = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1 = EventDate(
          relativeTimestamps(lessThanAgo = Duration.ofHours(23), moreThanAgo = Duration.ofMinutes(65)).generateOne
        )
        val lastSynced1 = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofMinutes(55)).generateOne)
        upsertProject(compoundId1, projectPath1, eventDate1)
        upsertLastSynced(compoundId1.projectId, categoryName, lastSynced1)

        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPath0))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return projects with a latest event date more than a day ago " +
      "and a last sync time more than a day ago " +
      "but not projects with a latest event date more than a day ago " +
      "and a last sync time less than a day ago" in new TestCase {
        val compoundId0  = compoundEventIds.generateOne
        val projectPath0 = projectPaths.generateOne
        val eventDate0   = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
        val lastSynced0  = LastSyncedDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
        upsertProject(compoundId0, projectPath0, eventDate0)
        upsertLastSynced(compoundId0.projectId, categoryName, lastSynced0)

        val compoundId1  = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = EventDate(relativeTimestamps(moreThanAgo = Duration.ofHours(25)).generateOne)
        val lastSynced1  = LastSyncedDate(relativeTimestamps(lessThanAgo = Duration.ofHours(23)).generateOne)
        upsertProject(compoundId1, projectPath1, eventDate1)
        upsertLastSynced(compoundId1.projectId, categoryName, lastSynced1)

        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPath0))
        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")

    val finder = new MemberSyncEventFinderImpl(sessionResource, queriesExecTimes)
  }
}
