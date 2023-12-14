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

package io.renku.eventlog.events.producers.tsmigrationrequest

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigrationGenerators.changeDates
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{ChangeDate, EventLogDB, EventLogPostgresSpec, MigrationStatus}
import io.renku.events.Generators.subscriberUrls
import io.renku.events.Subscription.SubscriberUrl
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.temporal.ChronoUnit.MICROS
import java.time.{Duration, Instant}

class EventFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues
    with TsMigrationTableProvisioning
    with ScalaCheckPropertyChecks {

  "pop - cases testing many versions" should {

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows with New for the same url but different versions" in testDBResource.use {
        implicit cfg =>
          val changeDate = changeDates.generateOne

          for {
            _ <- insertSubscriptionRecord(url, serviceVersions.generateOne, New, dateBefore(changeDate))

            _ <- insertSubscriptionRecord(url, version, New, changeDate)

            _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))
            _ <- findRow(url, version).asserting(_ shouldBe MigrationStatus.Sent -> ChangeDate(now))

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield ()
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for different urls and versions" in testDBResource.use { implicit cfg =>
        val changeDate = changeDates.generateOne

        for {
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne,
                                        serviceVersions.generateOne,
                                        New,
                                        dateBefore(changeDate)
               )
          _ <- insertSubscriptionRecord(url, version, New, changeDate)

          _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }

    "return no Event " +
      "- case when there are New and RecoverableFailure for older versions " +
      "but the most recent version has Done" in testDBResource.use { implicit cfg =>
        val changeDate = changeDates.generateOne

        for {
          // the most recent version records
          _ <- insertSubscriptionRecord(url, version, New, changeDate)
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, Done, dateAfter(changeDate))

          // older versions records
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne,
                                        serviceVersions.generateOne,
                                        New,
                                        dateBefore(changeDate)
               )
          _ <- insertSubscriptionRecord(url, serviceVersions.generateOne, RecoverableFailure, dateBefore(changeDate))

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }
  }

  "pop - cases testing different statuses within the recent version" should {

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version but any in Sent or Done" in testDBResource.use {
        implicit cfg =>
          val changeDate = changeDates.generateOne

          for {
            _ <- insertSubscriptionRecord(url, version, New, changeDate)
            _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateBefore(changeDate))
            _ <-
              insertSubscriptionRecord(subscriberUrls.generateOne, version, RecoverableFailure, dateBefore(changeDate))

            _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield ()
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version but any in Sent or Done " +
      "and the latest is in NonRecoverableFailure" in testDBResource.use { implicit cfg =>
        val changeDate = changeDates.generateOne

        for {
          _ <- insertSubscriptionRecord(url, version, New, changeDate)
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateBefore(changeDate))
          _ <-
            insertSubscriptionRecord(subscriberUrls.generateOne, version, NonRecoverableFailure, dateAfter(changeDate))

          _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version, any in Sent or Done " +
      "but some in RecoverableFailure for more than RecoverableStatusTimeout" in testDBResource.use { implicit cfg =>
        val changeDate = changeDates.generateOne

        for {
          _ <- insertSubscriptionRecord(url, version, New, changeDate)

          urlForRecoverable = subscriberUrls.generateOne
          _ <- insertSubscriptionRecord(urlForRecoverable,
                                        version,
                                        RecoverableFailure,
                                        more(than = recoverableStatusTimeout)
               )

          _ <- finder
                 .popEvent()
                 .asserting(_.value should {
                   be(MigrationRequestEvent(urlForRecoverable, version)) or be(MigrationRequestEvent(url, version))
                 })

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }

    "return no Event " +
      "- case when there are multiple rows for the same version, any in Sent, Done or New " +
      "but some in RecoverableFailure for less than RecoverableStatusTimeout" in testDBResource.use { implicit cfg =>
        val changeDate = changeDates.generateOne

        for {
          _ <- insertSubscriptionRecord(url, version, NonRecoverableFailure, changeDate)
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne,
                                        version,
                                        RecoverableFailure,
                                        less(than = recoverableStatusTimeout)
               )

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }

    "return Migration Request Event for the most recent row but not for the RecoverableFailure for less than RecoverableStatusTimeout " +
      "- case when there are multiple rows for the same version, any in Sent or Done but some in New" in testDBResource
        .use { implicit cfg =>
          val changeDate = changeDates.generateOne

          for {
            _ <- insertSubscriptionRecord(url, version, New, changeDate)
            _ <- insertSubscriptionRecord(subscriberUrls.generateOne,
                                          version,
                                          RecoverableFailure,
                                          less(than = recoverableStatusTimeout)
                 )

            _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield ()
        }

    "return no Event " +
      "- case when there are multiple rows for the same version, any in Done " +
      "but one in Sent for less than SentStatusTimeout" in testDBResource.use { implicit cfg =>
        val changeDate = changeDates.generateOne

        for {
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, New, changeDate)
          _ <- insertSubscriptionRecord(url, version, Sent, less(than = sentStatusTimeout, butAfter = changeDate))

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }

    "return no Event " +
      "- case when there are multiple rows for the same version " +
      "one in Sent for more than SentStatusTimeout" +
      "but also one in Done" in testDBResource.use { implicit cfg =>
        val newestVersionDate = more(than = sentStatusTimeout)
        for {
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateAfter(newestVersionDate))
          _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, Done, dateAfter(newestVersionDate))
          _ <- insertSubscriptionRecord(url, version, Sent, newestVersionDate)

          _ <- finder.popEvent().asserting(_ shouldBe None)
        } yield ()
      }

    "return Migration Request Event for the most recent version row with Sent " +
      "- case when there are multiple rows for the same version " +
      "and there's one with Sent for more than SentStatusTimeout" in testDBResource.use { implicit cfg =>
        val changeDate = more(than = sentStatusTimeout)
        val newUrl     = subscriberUrls.generateOne
        val newUrlDate = dateAfter(changeDate)

        for {
          _ <- insertSubscriptionRecord(newUrl, version, New, newUrlDate)
          _ <- insertSubscriptionRecord(url, version, Sent, changeDate)

          _ <- findRows(version).asserting(_ shouldBe Set((newUrl, New, newUrlDate), (url, Sent, changeDate)))

          _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))
          _ <- findRows(version).asserting(_ shouldBe Set((newUrl, New, newUrlDate), (url, Sent, ChangeDate(now))))

          _ <- finder.popEvent().asserting(_ shouldBe None)
          _ <- findRows(version).asserting(_ shouldBe Set((newUrl, New, newUrlDate), (url, Sent, ChangeDate(now))))
        } yield ()
      }

    "return no Event " +
      "- case when there are multiple rows for the same version " +
      "one in Sent for less than SentStatusTimeout" +
      "but also one in RecoverableFailure for more than RecoverableStatusTimeout" in testDBResource.use {
        implicit cfg =>
          for {
            _ <- insertSubscriptionRecord(subscriberUrls.generateOne, version, Sent, less(than = sentStatusTimeout))
            _ <- insertSubscriptionRecord(subscriberUrls.generateOne,
                                          version,
                                          RecoverableFailure,
                                          more(than = recoverableStatusTimeout)
                 )
            _ <- insertSubscriptionRecord(url, version, New, ChangeDate(now))

            _ <- finder.popEvent().asserting(_ shouldBe None)
          } yield ()
      }

    "return Migration Request Event for the most recent version row with Sent " +
      "- case when there are multiple rows for the same version " +
      "one in Sent for more than SentStatusTimeout " +
      "but also one in RecoverableFailure for more than RecoverableStatusTimeout" in testDBResource.use {
        implicit cfg =>
          val changeDate  = changeDates.generateOne
          val sentUrlDate = more(than = sentStatusTimeout)

          for {
            _ <- insertSubscriptionRecord(url, version, Sent, sentUrlDate)
            failedUrl     = subscriberUrls.generateOne
            failedUrlDate = more(than = recoverableStatusTimeout)
            _ <- insertSubscriptionRecord(failedUrl, version, RecoverableFailure, failedUrlDate)

            newUrl = subscriberUrls.generateOne
            _ <- insertSubscriptionRecord(newUrl, version, New, changeDate)

            _ <- findRows(version).asserting(
                   _ shouldBe Set(
                     (url, Sent, sentUrlDate),
                     (failedUrl, RecoverableFailure, failedUrlDate),
                     (newUrl, New, changeDate)
                   )
                 )

            _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))
            _ <- findRows(version).asserting(
                   _ shouldBe Set(
                     (url, Sent, ChangeDate(now)),
                     (failedUrl, RecoverableFailure, failedUrlDate),
                     (newUrl, New, changeDate)
                   )
                 )

            _ <- finder.popEvent().asserting(_ shouldBe None)
            _ <- findRows(version).asserting(
                   _ shouldBe Set(
                     (url, Sent, ChangeDate(now)),
                     (failedUrl, RecoverableFailure, failedUrlDate),
                     (newUrl, New, changeDate)
                   )
                 )
          } yield ()
      }

    "return Migration Request Event for the most recent version row with Sent " +
      "- case when there are multiple rows for the same version " +
      "one in Sent for more than SentStatusTimeout " +
      "but also one in RecoverableFailure older than the one in Sent" in testDBResource.use { implicit cfg =>
        val sentUrlDate = more(than = sentStatusTimeout)
        for {
          _ <- insertSubscriptionRecord(url, version, Sent, sentUrlDate)
          failedUrl     = subscriberUrls.generateOne
          failedUrlDate = dateBefore(sentUrlDate)
          _ <- insertSubscriptionRecord(failedUrl, version, RecoverableFailure, failedUrlDate)
          newUrl  = subscriberUrls.generateOne
          newDate = dateBefore(failedUrlDate)
          _ <- insertSubscriptionRecord(newUrl, version, New, newDate)

          _ <- findRows(version).asserting(
                 _ shouldBe Set(
                   (url, Sent, sentUrlDate),
                   (failedUrl, RecoverableFailure, failedUrlDate),
                   (newUrl, New, newDate)
                 )
               )

          _ <- finder.popEvent().asserting(_.value shouldBe MigrationRequestEvent(url, version))
          _ <- findRows(version).asserting(
                 _ shouldBe Set(
                   (url, Sent, ChangeDate(now)),
                   (failedUrl, RecoverableFailure, failedUrlDate),
                   (newUrl, New, newDate)
                 )
               )

          _ <- finder.popEvent().asserting(_ shouldBe None)
          _ <- findRows(version).asserting(
                 _ shouldBe Set(
                   (url, Sent, ChangeDate(now)),
                   (failedUrl, RecoverableFailure, failedUrlDate),
                   (newUrl, New, newDate)
                 )
               )
        } yield ()
      }

    "return no Event " +
      "- case when there is a single NonRecoverableFailure for the most recent version" in testDBResource.use {
        implicit cfg =>
          insertSubscriptionRecord(subscriberUrls.generateOne,
                                   version,
                                   NonRecoverableFailure,
                                   changeDates.generateOne
          ) >>
            finder.popEvent().asserting(_ shouldBe None)
      }
  }

  "pop" should {

    val changeDate = changeDates.generateOne

    forAll { (url1: SubscriberUrl, url2: SubscriberUrl, version: ServiceVersion) =>
      "never return more than one event for a single version " +
        "- case when there are multiple urls for the most recent version coming at the very same time " +
        s"- $url1, $url2, $version" in testDBResource.use { implicit cfg =>
          def singleEvent(subscriberUrl: SubscriberUrl) =
            insertSubscriptionRecord(subscriberUrl, version, New, changeDate) >>
              finder.popEvent()

          val result = (singleEvent(url1), singleEvent(url2)).parTupled
            .map(_.bimap(_.isDefined, _.isDefined))

          result.asserting(_ should { be(false -> true) or be(true -> false) }) >>
            List(url1, url2).map(findRow(_, version).map(_._1)).sequence.asserting(_.toSet shouldBe Set(New, Sent))
        }
    }
  }

  private lazy val url     = subscriberUrls.generateOne
  private lazy val version = serviceVersions.generateOne
  private lazy val now     = Instant.now().truncatedTo(MICROS)

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val wet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new EventFinderImpl[IO](() => now)
  }

  private def dateAfter(date: ChangeDate) =
    timestampsNotInTheFuture(butYoungerThan = date.value).generateAs(ChangeDate)

  private def less(than: Duration) =
    timestampsNotInTheFuture(butYoungerThan = Instant.now() minus than).generateAs(ChangeDate)

  private def less(than: Duration, butAfter: ChangeDate) =
    if ((Duration.between(butAfter.value, Instant.now()) compareTo than) < 0)
      timestampsNotInTheFuture(butYoungerThan = butAfter.value).generateAs(ChangeDate)
    else
      timestampsNotInTheFuture(butYoungerThan = Instant.now() minus than).generateAs(ChangeDate)

  private def more(than: Duration) =
    timestamps(max = Instant.now().minus(than).minusSeconds(1)).generateAs(ChangeDate)

  private def dateBefore(date: ChangeDate) =
    timestamps(max = date.value.minusSeconds(1)).generateAs(ChangeDate)

  private lazy val sentStatusTimeout        = Duration ofMinutes 1
  private lazy val recoverableStatusTimeout = Duration ofSeconds 30
}
