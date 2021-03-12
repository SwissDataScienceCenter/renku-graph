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
package commitsync

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.{EventDate, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class CommitSyncEventFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return the event for the project with the latest event date " +
      s"when the subscription_category_sync_times table has no rows for the $categoryName" in new TestCase {

        finder.popEvent().unsafeRunSync() shouldBe None

        val event0Id          = compoundEventIds.generateOne
        val event0Date        = eventDates.generateOne
        val event0ProjectPath = projectPaths.generateOne
        addEvent(event0Id, event0Date, event0ProjectPath)

        val event1Id          = compoundEventIds.generateOne
        val event1Date        = eventDates.generateOne
        val event1ProjectPath = projectPaths.generateOne
        addEvent(event1Id, event1Date, event1ProjectPath)
        upsertLastSynced(event1Id.projectId,
                         membersync.categoryName,
                         relativeTimestamps(lessThanAgo = Duration.ofMillis(30)).generateAs(LastSyncedDate)
        )

        List(
          (event0Id, event0ProjectPath, event0Date),
          (event1Id, event1ProjectPath, event1Date)
        ).sortBy(_._3).reverse foreach { case (eventId, path, eventDate) =>
          finder.popEvent().unsafeRunSync() shouldBe Some(
            CommitSyncEvent(eventId, path, LastSyncedDate(eventDate.value))
          )
        }
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return events for " +
      "projects with a latest event date less than a week ago " +
      "and a last sync time more than an hour ago " +
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

        finder.popEvent().unsafeRunSync() shouldBe Some(CommitSyncEvent(event0Id, event0ProjectPath, event0LastSynced))
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

        finder.popEvent().unsafeRunSync() shouldBe Some(CommitSyncEvent(event0Id, event0ProjectPath, event0LastSynced))
        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    val finder = new CommitSyncEventFinderImpl(transactor, TestLabeledHistogram[SqlQuery.Name]("query_id"))
  }

  private def addEvent(eventId: CompoundEventId, eventDate: EventDate, projectPath: projects.Path): Unit =
    storeEvent(eventId,
               eventStatuses.generateOne,
               executionDates.generateOne,
               eventDate,
               eventBodies.generateOne,
               projectPath = projectPath
    )
}
