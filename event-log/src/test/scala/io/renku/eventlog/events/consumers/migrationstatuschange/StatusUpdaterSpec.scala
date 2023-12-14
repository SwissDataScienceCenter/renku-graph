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

package io.renku.eventlog.events.consumers.migrationstatuschange

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigrationGenerators.changeDates
import io.renku.eventlog._
import io.renku.eventlog.events.consumers.migrationstatuschange.Event.{ToDone, ToNonRecoverableFailure, ToRecoverableFailure}
import io.renku.eventlog.events.producers.tsmigrationrequest.TsMigrationTableProvisioning
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.events.Generators.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.sentences
import org.scalacheck.Gen.asciiPrintableChar
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class StatusUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with TsMigrationTableProvisioning
    with TableDrivenPropertyChecks
    with should.Matchers {

  "updateStatus" should {

    Set(
      ToDone(url, version),
      ToRecoverableFailure(url, version, message),
      ToNonRecoverableFailure(url, version, message)
    ) foreach { event =>
      s"succeed updating to ${event.newStatus} only if corresponding record in Sent" in testDBResource.use {
        implicit cfg =>
          for {
            _ <- insertSubscriptionRecord(url, version, Sent, changeDate).assertNoException

            _ <- updater.updateStatus(event).assertNoException

            _ <- findRow(url, version).asserting(_ shouldBe event.newStatus -> ChangeDate(now))

            _ <- if (Set(RecoverableFailure, NonRecoverableFailure) contains event.newStatus)
                   findMessage(url, version).asserting(_ shouldBe message.some)
                 else
                   findMessage(url, version).asserting(_ shouldBe None)
          } yield Succeeded
      }
    }

    MigrationStatus.all - Sent foreach { status =>
      s"do nothing and succeed if corresponding record is not in $status" in testDBResource.use { implicit cfg =>
        for {
          _ <- insertSubscriptionRecord(url, version, status, changeDate).assertNoException

          _ <- updater.updateStatus(ToDone(url, version)).assertNoException

          _ <- findRow(url, version).asserting(_ shouldBe status -> changeDate)
        } yield Succeeded
      }
    }

    "do nothing and succeed if corresponding record does not exist" in testDBResource.use { implicit cfg =>
      updater.updateStatus(ToDone(url, version)).assertNoException
    }

    "do update only corresponding record" in testDBResource.use { implicit cfg =>
      for {
        _ <- insertSubscriptionRecord(url, version, Sent, changeDate).assertNoException

        otherUrl = subscriberUrls.generateOne
        _ <- insertSubscriptionRecord(otherUrl, version, Sent, changeDate).assertNoException

        otherVersion = serviceVersions.generateOne
        _ <- insertSubscriptionRecord(url, otherVersion, Sent, changeDate).assertNoException

        _ <- updater.updateStatus(ToDone(url, version)).assertNoException

        _ <- findRow(url, version).asserting(_ shouldBe Done -> ChangeDate(now))
        _ <- findRow(otherUrl, version).asserting(_ shouldBe Sent -> changeDate)
        _ <- findRow(url, otherVersion).asserting(_ shouldBe Sent -> changeDate)
      } yield Succeeded
    }
  }

  private lazy val url        = subscriberUrls.generateOne
  private lazy val version    = serviceVersions.generateOne
  private lazy val message    = sentences(charsGenerator = asciiPrintableChar).map(_.value).generateAs(MigrationMessage)
  private lazy val changeDate = changeDates.generateOne
  private lazy val now        = Instant.now().truncatedTo(MICROS)

  private def updater(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new StatusUpdaterImpl[IO](() => now)
  }
}
