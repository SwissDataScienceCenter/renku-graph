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

package io.renku.eventlog.subscriptions.tsmigrationrequest

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.tsmigrationrequest.MigrationStatus._
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant.now
import java.time.{Duration, Instant}

class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TsMigrationTableProvisioning
    with MockFactory
    with should.Matchers {

  "pop - cases testing many versions" should {

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows with New for the same url but different versions" in new TestCase {

        insertSubscriptionRecord(url, serviceVersions.generateOne, New, dateBefore(changeDate))

        insertSubscriptionRecord(url, version, New, changeDate)

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some
        findRows(url, version)            shouldBe MigrationStatus.Sent -> ChangeDate(now)

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for different urls and versions" in new TestCase {

        insertSubscriptionRecord(subscriberUrls.generateOne, serviceVersions.generateOne, New, dateBefore(changeDate))

        insertSubscriptionRecord(url, version, New, changeDate)

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no Event " +
      "- case when there are New for older versions " +
      "but the most recent version has Done" in new TestCase {

        // the most recent version records
        insertSubscriptionRecord(url, version, New, changeDate)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, Done, dateAfter(changeDate))

        // older versions records
        insertSubscriptionRecord(subscriberUrls.generateOne, serviceVersions.generateOne, New, dateBefore(changeDate))

        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  "pop - cases testing different statuses within the recent version" should {

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version but any in Sent or Done" in new TestCase {

        insertSubscriptionRecord(url, version, New, changeDate)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateBefore(changeDate))

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version but any in Sent or Done " +
      "and the latest is in Failure" in new TestCase {

        insertSubscriptionRecord(url, version, New, changeDate)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateBefore(changeDate))
        insertSubscriptionRecord(subscriberUrls.generateOne, version, Failure, dateAfter(changeDate))

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no Event " +
      "- case when there are multiple rows for the same version, any in Done " +
      "but one in Sent for less than an hour" in new TestCase {

        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, changeDate)
        insertSubscriptionRecord(url, version, Sent, less(than = anHour, butAfter = changeDate))

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no Event " +
      "- case when there are multiple rows for the same version " +
      "one in Sent for more than an hour" +
      "but also one in Done" in new TestCase {

        val newestVersionDate = more(than = anHour)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateAfter(newestVersionDate))
        insertSubscriptionRecord(subscriberUrls.generateOne, version, Done, dateAfter(newestVersionDate))
        insertSubscriptionRecord(url, version, Sent, newestVersionDate)

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent version row with Sent " +
      "- case when there are multiple rows for the same version " +
      "and there's one with Sent for more than an hour" in new TestCase {

        val newestVersionDate = more(than = anHour)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateAfter(newestVersionDate))
        insertSubscriptionRecord(url, version, Sent, newestVersionDate)

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no Event " +
      "- case when there is a single Failure for the most recent version" in new TestCase {

        insertSubscriptionRecord(subscriberUrls.generateOne, version, Failure, changeDate)

        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  private trait TestCase {
    val url        = subscriberUrls.generateOne
    val version    = serviceVersions.generateOne
    val changeDate = changeDates.generateOne
    val now        = Instant.now()

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val currentTime      = mockFunction[Instant]
    val finder           = new EventFinder[IO](queriesExecTimes, currentTime)

    currentTime.expects().returning(now).anyNumberOfTimes()
  }

  private def dateAfter(date: ChangeDate) =
    timestampsNotInTheFuture(butYoungerThan = date.value).generateAs(ChangeDate)

  private def less(than: Duration, butAfter: ChangeDate) =
    if ((Duration.between(butAfter.value, now()) compareTo than) < 0)
      timestampsNotInTheFuture(butYoungerThan = butAfter.value).generateAs(ChangeDate)
    else
      timestampsNotInTheFuture(butYoungerThan = now() minus than).generateAs(ChangeDate)

  private def more(than: Duration) =
    timestamps(max = now().minus(than).minusSeconds(1)).generateAs(ChangeDate)

  private def dateBefore(date: ChangeDate) =
    timestamps(max = date.value.minusSeconds(1)).generateAs(ChangeDate)

  private lazy val anHour = Duration ofHours 1
}
