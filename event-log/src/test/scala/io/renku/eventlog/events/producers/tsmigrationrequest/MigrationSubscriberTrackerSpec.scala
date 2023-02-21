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

import Generators._
import cats.data.Kleisli
import cats.effect.IO
import io.renku.config.ServiceVersion
import io.renku.eventlog._
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigtationTypeSerializers._
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.Generators.{subscriberIds, subscriberUrls}
import io.renku.events.Subscription.SubscriberUrl
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class MigrationSubscriberTrackerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "add" should {

    "insert a new row into the ts_migration table " +
      "if there's no row for the version " +
      "and the version -> url pair does not exists" in new TestCase {

        (tracker add subscriber).unsafeRunSync() shouldBe true

        findRows(subscriber.url) shouldBe List(
          (subscriber.url, subscriber.version, New, ChangeDate(now), None)
        )
      }

    "do nothing if there's already a row for the version -> url pair" in new TestCase {

      (tracker add subscriber).unsafeRunSync() shouldBe true

      val differentIdInfo = subscriber.copy(id = subscriberIds.generateOne)
      (tracker add differentIdInfo).unsafeRunSync() shouldBe true

      findRows(subscriber.url) shouldBe List(
        (subscriber.url, subscriber.version, New, ChangeDate(now), None)
      )
    }

    "insert a new row " +
      "if there's a different version for the same url" in new TestCase {

        (tracker add subscriber).unsafeRunSync() shouldBe true

        val differentVersionInfo = subscriber.copy(version = serviceVersions.generateOne)
        (tracker add differentVersionInfo).unsafeRunSync() shouldBe true

        findRows(subscriber.url) should contain theSameElementsAs List(
          (subscriber.url, subscriber.version, New, ChangeDate(now), None),
          (differentVersionInfo.url, differentVersionInfo.version, New, ChangeDate(now), None)
        )
      }

    "insert a new row " +
      "if there's a different url for the version already existing in the table " +
      "but all the other same version urls are not in DONE" in new TestCase {

        val otherStatusesInfos = (MigrationStatus.all - Done) map { status =>
          val sameVersionInfo = subscriber.copy(url = subscriberUrls.generateOne)
          (tracker add sameVersionInfo).unsafeRunSync() shouldBe true

          updateStatus(sameVersionInfo, status)

          status -> sameVersionInfo
        }

        (tracker add subscriber).unsafeRunSync() shouldBe true

        findRows(subscriber.url) shouldBe List(
          (subscriber.url, subscriber.version, New, ChangeDate(now), None)
        )
        otherStatusesInfos.map { case (status, info) =>
          findRows(info.url) shouldBe List((info.url, info.version, status, ChangeDate(now), None))
        }
      }

    "do not add a new row " +
      "if there's at least one url for the version already in status DONE" in new TestCase {

        val sameVersionInfoDone = subscriber.copy(url = subscriberUrls.generateOne)
        (tracker add sameVersionInfoDone).unsafeRunSync() shouldBe true

        updateStatus(sameVersionInfoDone, Done)

        (tracker add subscriber).unsafeRunSync() shouldBe false

        findRows(sameVersionInfoDone.url) shouldBe List(
          (sameVersionInfoDone.url, sameVersionInfoDone.version, Done, ChangeDate(now), None)
        )
        findRows(subscriber.url) shouldBe Nil
      }
  }

  "remove" should {

    New :: Sent :: NonRecoverableFailure :: RecoverableFailure :: Nil foreach { status =>
      s"delete version -> url pair if exists and not in status $status" in new TestCase {

        (tracker add subscriber).unsafeRunSync() shouldBe true

        updateStatus(subscriber, status)

        (tracker remove subscriber.url).unsafeRunSync() shouldBe true

        findRows(subscriber.url) shouldBe Nil
      }
    }

    "do not remove version -> url pairs with status DONE" in new TestCase {

      (tracker add subscriber).unsafeRunSync() shouldBe true
      updateStatus(subscriber, Done)

      val sameUrlDifferentVersion = subscriber.copy(version = serviceVersions.generateOne)
      (tracker add sameUrlDifferentVersion).unsafeRunSync() shouldBe true
      updateStatus(sameUrlDifferentVersion, Done)

      val sameUrlDifferentVersionNotDone1 = subscriber.copy(version = serviceVersions.generateOne)
      (tracker add sameUrlDifferentVersionNotDone1).unsafeRunSync() shouldBe true

      val sameUrlDifferentVersionNotDone2 = subscriber.copy(version = serviceVersions.generateOne)
      (tracker add sameUrlDifferentVersionNotDone2).unsafeRunSync() shouldBe true

      (tracker remove subscriber.url).unsafeRunSync() shouldBe true

      findRows(subscriber.url) should contain theSameElementsAs List(
        (subscriber.url, subscriber.version, Done, ChangeDate(now), None),
        (subscriber.url, sameUrlDifferentVersion.version, Done, ChangeDate(now), None)
      )
    }

    "do nothing for a non-existing url" in new TestCase {
      (tracker remove subscriberUrls.generateOne).unsafeRunSync() shouldBe true
    }
  }

  private trait TestCase {
    val subscriber = migrationSubscribers.generateOne
    val now        = Instant.now().truncatedTo(MICROS)

    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    private val currentTime = mockFunction[Instant]
    val tracker             = new MigrationSubscriberTracker[IO](currentTime)

    currentTime.expects().returning(now).anyNumberOfTimes()
  }

  private def findRows(
      url: SubscriberUrl
  ): List[(SubscriberUrl, ServiceVersion, MigrationStatus, ChangeDate, Option[MigrationMessage])] = execute {
    Kleisli { session =>
      val query: Query[SubscriberUrl,
                       (SubscriberUrl, ServiceVersion, MigrationStatus, ChangeDate, Option[MigrationMessage])
      ] =
        sql"""SELECT subscriber_url, subscriber_version, status, change_date, message
              FROM ts_migration
              WHERE subscriber_url = $subscriberUrlEncoder"""
          .query(
            subscriberUrlDecoder ~ serviceVersionDecoder ~ migrationStatusDecoder ~ changeDateDecoder ~ migrationMessageDecoder.opt
          )
          .map { case url ~ version ~ status ~ changeDate ~ maybeMessage =>
            (url, version, status, changeDate, maybeMessage)
          }
      session.prepare(query).flatMap(_.stream(url, 32).compile.toList)
    }
  }

  private def updateStatus(info: MigrationSubscriber, status: MigrationStatus): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[MigrationStatus ~ SubscriberUrl ~ ServiceVersion] =
        sql"""UPDATE ts_migration
              SET status = $migrationStatusEncoder
              WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder
          """.command
      session
        .prepare(query)
        .flatMap(_.execute(status ~ info.url ~ info.version))
        .void
    }
  }
}
