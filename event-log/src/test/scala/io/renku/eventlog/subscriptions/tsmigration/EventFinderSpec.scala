package io.renku.eventlog.subscriptions.tsmigration

import Generators._
import TypeSerializers._
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.tsmigration.MigrationStatus._
import io.renku.events.consumers.subscriptions.{SubscriberUrl, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.http.server.version.ServiceVersion
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.implicits._
import skunk.{Command, Query, ~}

import java.time.Instant.now
import java.time.{Duration, Instant}

class EventFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
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
      "- case when there are New and RecoverableFailure for older versions " +
      "but the most recent version has Done" in new TestCase {

        // the most recent version records
        insertSubscriptionRecord(url, version, New, changeDate)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, Done, dateAfter(changeDate))

        // older versions records
        insertSubscriptionRecord(subscriberUrls.generateOne, serviceVersions.generateOne, New, dateBefore(changeDate))
        insertSubscriptionRecord(url, serviceVersions.generateOne, RecoverableFailure, dateBefore(changeDate))

        finder.popEvent().unsafeRunSync() shouldBe None
      }
  }

  "pop - cases testing different statuses within the recent version" should {

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version but any in Sent or Done" in new TestCase {

        insertSubscriptionRecord(url, version, New, changeDate)

        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateBefore(changeDate))

        insertSubscriptionRecord(subscriberUrls.generateOne, version, RecoverableFailure, dateBefore(changeDate))

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version but any in Sent or Done " +
      "and the latest is in NonRecoverableFailure" in new TestCase {

        insertSubscriptionRecord(url, version, New, changeDate)

        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, dateBefore(changeDate))

        insertSubscriptionRecord(subscriberUrls.generateOne, version, NonRecoverableFailure, dateAfter(changeDate))

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent row " +
      "- case when there are multiple rows for the same version, any in Sent or Done " +
      "but some in RecoverableFailure for more than 2 mins" in new TestCase {

        insertSubscriptionRecord(url, version, New, changeDate)

        val urlForRecoverable = subscriberUrls.generateOne
        insertSubscriptionRecord(urlForRecoverable, version, RecoverableFailure, more(than = twoMins))

        finder.popEvent().unsafeRunSync() should {
          be(MigrationRequestEvent(urlForRecoverable, version).some) or
            be(MigrationRequestEvent(url, version).some)
        }

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no Event " +
      "- case when there are multiple rows for the same version, any in Sent, Done or New " +
      "but some in RecoverableFailure for less than 2 mins" in new TestCase {

        insertSubscriptionRecord(url, version, NonRecoverableFailure, changeDate)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, RecoverableFailure, less(than = twoMins))

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent row but not for the RecoverableFailure for less than 2 mins " +
      "- case when there are multiple rows for the same version, any in Sent or Done but some in New" in new TestCase {

        insertSubscriptionRecord(url, version, New, changeDate)
        insertSubscriptionRecord(subscriberUrls.generateOne, version, RecoverableFailure, less(than = twoMins))

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
      "- case when there are multiple rows for the same version " +
      "one in Sent for less than an hour" +
      "but also one in RecoverableFailure for more than 2 mins" in new TestCase {

        insertSubscriptionRecord(subscriberUrls.generateOne, version, Sent, less(than = anHour))
        insertSubscriptionRecord(subscriberUrls.generateOne, version, RecoverableFailure, more(than = twoMins))
        insertSubscriptionRecord(url, version, New, ChangeDate(now))

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return Migration Request Event for the most recent version row with Sent " +
      "- case when there are multiple rows for the same version " +
      "one in Sent for more than an hour" +
      "but also one in RecoverableFailure for more than 2 mins" in new TestCase {

        insertSubscriptionRecord(url, version, Sent, more(than = anHour))
        insertSubscriptionRecord(subscriberUrls.generateOne, version, RecoverableFailure, more(than = twoMins))
        insertSubscriptionRecord(subscriberUrls.generateOne, version, New, changeDate)

        finder.popEvent().unsafeRunSync() shouldBe MigrationRequestEvent(url, version).some

        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return no Event " +
      "- case when there is a single NonRecoverableFailure for the most recent version" in new TestCase {

        insertSubscriptionRecord(subscriberUrls.generateOne, version, NonRecoverableFailure, changeDate)

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

  private def insertSubscriptionRecord(url:        SubscriberUrl,
                                       version:    ServiceVersion,
                                       status:     MigrationStatus,
                                       changeDate: ChangeDate
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[ServiceVersion ~ SubscriberUrl ~ MigrationStatus ~ ChangeDate] = sql"""
        INSERT INTO ts_migration (subscriber_version, subscriber_url, status, change_date)
        VALUES ($serviceVersionEncoder, $subscriberUrlEncoder, $migrationStatusEncoder, $changeDateEncoder)
        """.command
      session
        .prepare(query)
        .use(_.execute(version ~ url ~ status ~ changeDate))
        .void
    }
  }

  private def findRows(url: SubscriberUrl, version: ServiceVersion): (MigrationStatus, ChangeDate) = execute {
    Kleisli { session =>
      val query: Query[SubscriberUrl ~ ServiceVersion, (MigrationStatus, ChangeDate)] =
        sql"""SELECT status, change_date
              FROM ts_migration
              WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder"""
          .query(migrationStatusDecoder ~ changeDateDecoder)
          .map { case status ~ changeDate => status -> changeDate }
      session.prepare(query).use(_.unique(url ~ version))
    }
  }

  private def dateAfter(date: ChangeDate) =
    timestampsNotInTheFuture(butYoungerThan = date.value).generateAs(ChangeDate)

  private def less(than: Duration) =
    timestampsNotInTheFuture(butYoungerThan = now() minus than).generateAs(ChangeDate)

  private def less(than: Duration, butAfter: ChangeDate) =
    if ((Duration.between(butAfter.value, now()) compareTo than) < 0)
      timestampsNotInTheFuture(butYoungerThan = butAfter.value).generateAs(ChangeDate)
    else
      timestampsNotInTheFuture(butYoungerThan = now() minus than).generateAs(ChangeDate)

  private def more(than: Duration) =
    timestamps(max = now().minus(than).minusSeconds(1)).generateAs(ChangeDate)

  private def dateBefore(date: ChangeDate) =
    timestamps(max = date.value.minusSeconds(1)).generateAs(ChangeDate)

  private lazy val anHour  = Duration ofHours 1
  private lazy val twoMins = Duration ofMinutes 2
}
