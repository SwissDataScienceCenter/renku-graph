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

import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.tsmigrationrequest.Generators.changeDates
import io.renku.eventlog.subscriptions.tsmigrationrequest.MigrationStatus._
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class DispatchRecoverySpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TsMigrationTableProvisioning
    with MockFactory
    with should.Matchers {

  "returnToQueue" should {

    "change the status of the corresponding row in the ts_migration table to New if it was in Sent" in new TestCase {
      insertSubscriptionRecord(url, version, Sent, changeDate)

      recovery.returnToQueue(MigrationRequestEvent(url, version)).unsafeRunSync() shouldBe ()

      findRows(url, version) shouldBe New -> ChangeDate(now)
    }

    MigrationStatus.all - Sent foreach { status =>
      s"do no change the status of the corresponding row if it's in $status" in new TestCase {
        insertSubscriptionRecord(url, version, status, changeDate)

        recovery.returnToQueue(MigrationRequestEvent(url, version)).unsafeRunSync() shouldBe ()

        findRows(url, version) shouldBe status -> changeDate
      }
    }

    "do no change the status of rows other than the one pointed by the event" in new TestCase {
      val status = Gen.oneOf(MigrationStatus.all).generateOne
      insertSubscriptionRecord(url, version, status, changeDate)

      recovery
        .returnToQueue(MigrationRequestEvent(subscriberUrls.generateOne, serviceVersions.generateOne))
        .unsafeRunSync() shouldBe ()

      findRows(url, version) shouldBe status -> changeDate
    }
  }

  "recover" should {

    val exception = exceptions.generateOne

    "change status of the relevant row to NonRecoverableFailure if in Sent" in new TestCase {
      insertSubscriptionRecord(url, version, Sent, changeDate)

      recovery.recover(url, MigrationRequestEvent(url, version))(exception).unsafeRunSync() shouldBe ()

      findRows(url, version) shouldBe NonRecoverableFailure -> ChangeDate(now)

      logger.loggedOnly(Info(s"TS_MIGRATION_REQUEST - recovering from ${exception.getMessage}"))
    }

    MigrationStatus.all - Sent foreach { status =>
      s"do no change the status of the corresponding row if it's in $status" in new TestCase {
        insertSubscriptionRecord(url, version, status, changeDate)

        recovery.recover(url, MigrationRequestEvent(url, version))(exception).unsafeRunSync() shouldBe ()

        findRows(url, version) shouldBe status -> changeDate
      }
    }

    "do no change the status of rows other than the one pointed by the event" in new TestCase {
      val status = Gen.oneOf(MigrationStatus.all).generateOne
      insertSubscriptionRecord(url, version, status, changeDate)

      val failingEvent = MigrationRequestEvent(subscriberUrls.generateOne, serviceVersions.generateOne)

      recovery.recover(failingEvent.subscriberUrl, failingEvent)(exception).unsafeRunSync() shouldBe ()

      findRows(url, version) shouldBe status -> changeDate
    }
  }

  private trait TestCase {
    val url        = subscriberUrls.generateOne
    val version    = serviceVersions.generateOne
    val changeDate = changeDates.generateOne
    val now        = Instant.now()

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val currentTime      = mockFunction[Instant]
    val recovery         = new DispatchRecoveryImpl[IO](queriesExecTimes, currentTime)

    currentTime.expects().returning(now).anyNumberOfTimes()
  }
}
