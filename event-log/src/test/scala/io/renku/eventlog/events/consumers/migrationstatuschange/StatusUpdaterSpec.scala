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

package io.renku.eventlog.events.consumers.migrationstatuschange

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigrationGenerators.changeDates
import io.renku.eventlog._
import io.renku.eventlog.events.consumers.migrationstatuschange.Event.{ToDone, ToNonRecoverableFailure, ToRecoverableFailure}
import io.renku.eventlog.events.producers.tsmigrationrequest.TsMigrationTableProvisioning
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.sentences
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class StatusUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TsMigrationTableProvisioning
    with MockFactory
    with TableDrivenPropertyChecks
    with should.Matchers {

  "updateStatus" should {

    Set(
      ToDone(url, version),
      ToRecoverableFailure(url, version, message),
      ToNonRecoverableFailure(url, version, message)
    ) foreach { event =>
      s"succeed updating to ${event.newStatus} only if corresponding record in Sent" in new TestCase {
        insertSubscriptionRecord(url, version, Sent, changeDate)

        updater.updateStatus(event).unsafeRunSync() shouldBe ()

        findRow(url, version) shouldBe event.newStatus -> ChangeDate(now)

        if (Set(RecoverableFailure, NonRecoverableFailure) contains event.newStatus)
          findMessage(url, version) shouldBe message.some
        else
          findMessage(url, version) shouldBe None
      }
    }

    MigrationStatus.all - Sent foreach { status =>
      s"do nothing and succeed if corresponding record is not in $status" in new TestCase {
        insertSubscriptionRecord(url, version, status, changeDate)

        updater.updateStatus(ToDone(url, version)).unsafeRunSync() shouldBe ()

        findRow(url, version) shouldBe status -> changeDate
      }
    }

    "do nothing and succeed if corresponding record does not exist" in new TestCase {
      updater.updateStatus(ToDone(url, version)).unsafeRunSync() shouldBe ()
    }

    "do update only corresponding record" in new TestCase {
      insertSubscriptionRecord(url, version, Sent, changeDate)

      val otherUrl = subscriberUrls.generateOne
      insertSubscriptionRecord(otherUrl, version, Sent, changeDate)

      val otherVersion = serviceVersions.generateOne
      insertSubscriptionRecord(url, otherVersion, Sent, changeDate)

      updater.updateStatus(ToDone(url, version)).unsafeRunSync() shouldBe ()

      findRow(url, version)      shouldBe Done -> ChangeDate(now)
      findRow(otherUrl, version) shouldBe Sent -> changeDate
      findRow(url, otherVersion) shouldBe Sent -> changeDate
    }
  }

  private lazy val url     = subscriberUrls.generateOne
  private lazy val version = serviceVersions.generateOne
  private lazy val message = sentences().map(_.value).generateAs(MigrationMessage)

  private trait TestCase {
    val changeDate = changeDates.generateOne
    val now        = Instant.now().truncatedTo(MICROS)

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val currentTime      = mockFunction[Instant]
    val updater          = new StatusUpdaterImpl[IO](queriesExecTimes, currentTime)

    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
