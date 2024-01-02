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

package io.renku.eventlog.events.producers
package tsmigrationrequest

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigrationGenerators.changeDates
import io.renku.eventlog._
import io.renku.eventlog.events.producers.EventsSender.SendingResult
import io.renku.eventlog.events.producers.EventsSender.SendingResult.TemporarilyUnavailable
import io.renku.eventlog.events.producers.Generators.sendingResults
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.events.Generators.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger.Level.Info
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class DispatchRecoverySpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with TsMigrationTableProvisioning
    with should.Matchers {

  "returnToQueue" should {

    "change the status of the corresponding row in the ts_migration table to New " +
      "if it was in Sent and the reason is NOT TemporarilyUnavailable" in testDBResource.use { implicit cfg =>
        insertSubscriptionRecord(url, version, Sent, changeDate) >>
          recovery
            .returnToQueue(MigrationRequestEvent(url, version), reason = notBusyStatus.generateOne)
            .assertNoException >>
          findRow(url, version).asserting(_ shouldBe New -> ChangeDate(now))
      }

    "leave the status of the corresponding row in the ts_migration table as Sent " +
      "and update the change_date " +
      "if it was in Sent and the reason is TemporarilyUnavailable" in testDBResource.use { implicit cfg =>
        insertSubscriptionRecord(url, version, Sent, changeDate) >>
          recovery
            .returnToQueue(MigrationRequestEvent(url, version), reason = TemporarilyUnavailable)
            .assertNoException >>
          findRow(url, version).asserting(_ shouldBe Sent -> ChangeDate(now))
      }

    MigrationStatus.all - Sent foreach { status =>
      s"do no change the status of the corresponding row if it's in $status" in testDBResource.use { implicit cfg =>
        insertSubscriptionRecord(url, version, status, changeDate) >>
          recovery
            .returnToQueue(MigrationRequestEvent(url, version), reason = sendingResults.generateOne)
            .assertNoException >>
          findRow(url, version).asserting(_ shouldBe status -> changeDate)
      }
    }

    "do no change the status of rows other than the one pointed by the event" in testDBResource.use { implicit cfg =>
      val status = Gen.oneOf(MigrationStatus.all).generateOne
      insertSubscriptionRecord(url, version, status, changeDate) >>
        recovery
          .returnToQueue(MigrationRequestEvent(subscriberUrls.generateOne, serviceVersions.generateOne),
                         reason = sendingResults.generateOne
          )
          .assertNoException >>
        findRow(url, version).asserting(_ shouldBe status -> changeDate)
    }
  }

  "recover" should {

    val exception = exceptions.generateOne

    "change status of the relevant row to NonRecoverableFailure if in Sent" in testDBResource.use { implicit cfg =>
      for {
        _ <- insertSubscriptionRecord(url, version, Sent, changeDate)

        _ <- logger.resetF()

        _ <- recovery.recover(url, MigrationRequestEvent(url, version))(exception).assertNoException

        _ <- findRow(url, version).asserting(_ shouldBe NonRecoverableFailure -> ChangeDate(now))
        _ <- findMessage(url, version).asserting(_ shouldBe MigrationMessage(exception).some)

        _ <- logger.loggedOnlyF(Info("TS_MIGRATION_REQUEST: recovering from NonRecoverable Failure", exception))
      } yield Succeeded
    }

    MigrationStatus.all - Sent foreach { status =>
      s"do no change the status of the corresponding row if it's in $status" in testDBResource.use { implicit cfg =>
        insertSubscriptionRecord(url, version, status, changeDate) >>
          recovery.recover(url, MigrationRequestEvent(url, version))(exception).assertNoException >>
          findRow(url, version).asserting(_ shouldBe status -> changeDate)
      }
    }

    "do no change the status of rows other than the one pointed by the event" in testDBResource.use { implicit cfg =>
      val status = Gen.oneOf(MigrationStatus.all).generateOne
      for {
        _ <- insertSubscriptionRecord(url, version, status, changeDate)

        failingEvent = MigrationRequestEvent(subscriberUrls.generateOne, serviceVersions.generateOne)

        _ <- recovery.recover(failingEvent.subscriberUrl, failingEvent)(exception).assertNoException

        _ <- findRow(url, version).asserting(_ shouldBe status -> changeDate)
      } yield Succeeded
    }
  }

  private lazy val url        = subscriberUrls.generateOne
  private lazy val version    = serviceVersions.generateOne
  private lazy val changeDate = changeDates.generateOne
  private lazy val now        = Instant.now().truncatedTo(MICROS)

  private def recovery(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DispatchRecoveryImpl[IO](() => now)
  }

  private lazy val notBusyStatus: Gen[SendingResult] =
    Gen.oneOf(
      EventsSender.SendingResult.all - TemporarilyUnavailable
    )
}
