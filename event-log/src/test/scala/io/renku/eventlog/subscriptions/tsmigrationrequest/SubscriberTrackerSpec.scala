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
import cats.data.Kleisli
import cats.effect.IO
import io.renku.config.ServiceVersion
import io.renku.db.SqlStatement
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigtationTypeSerializers._
import io.renku.eventlog._
import io.renku.events.consumers.subscriptions.{SubscriberUrl, subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class SubscriberTrackerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "add" should {

    "insert a new row into the ts_migration table " +
      "if there's no row for the version " +
      "and the version -> url pair does not exists" in new TestCase {

        (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

        findRows(subscriptionInfo.subscriberUrl) shouldBe List(
          (subscriptionInfo.subscriberUrl, subscriptionInfo.subscriberVersion, New, ChangeDate(now), None)
        )
      }

    "do nothing if there's already a row for the version -> url pair" in new TestCase {

      (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

      val differentIdInfo = subscriptionInfo.copy(subscriberId = subscriberIds.generateOne)
      (tracker add differentIdInfo).unsafeRunSync() shouldBe true

      findRows(subscriptionInfo.subscriberUrl) shouldBe List(
        (subscriptionInfo.subscriberUrl, subscriptionInfo.subscriberVersion, New, ChangeDate(now), None)
      )
    }

    "insert a new row " +
      "if there's a different version for the same url" in new TestCase {

        (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

        val differentVersionInfo = subscriptionInfo.copy(subscriberVersion = serviceVersions.generateOne)
        (tracker add differentVersionInfo).unsafeRunSync() shouldBe true

        findRows(subscriptionInfo.subscriberUrl) should contain theSameElementsAs List(
          (subscriptionInfo.subscriberUrl, subscriptionInfo.subscriberVersion, New, ChangeDate(now), None),
          (differentVersionInfo.subscriberUrl, differentVersionInfo.subscriberVersion, New, ChangeDate(now), None)
        )
      }

    "insert a new row " +
      "if there's a different url for the version already existing in the table " +
      "but all the other same version urls are not in DONE" in new TestCase {

        val otherStatusesInfos = (MigrationStatus.all - Done) map { status =>
          val sameVersionInfo = subscriptionInfo.copy(subscriberUrl = subscriberUrls.generateOne)
          (tracker add sameVersionInfo).unsafeRunSync() shouldBe true

          updateStatus(sameVersionInfo, status)

          status -> sameVersionInfo
        }

        (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

        findRows(subscriptionInfo.subscriberUrl) shouldBe List(
          (subscriptionInfo.subscriberUrl, subscriptionInfo.subscriberVersion, New, ChangeDate(now), None)
        )
        otherStatusesInfos.map { case (status, info) =>
          findRows(info.subscriberUrl) shouldBe List(
            (info.subscriberUrl, info.subscriberVersion, status, ChangeDate(now), None)
          )
        }
      }

    "do not add a new row " +
      "if there's at least one url for the version already in status DONE" in new TestCase {

        val sameVersionInfoDone = subscriptionInfo.copy(subscriberUrl = subscriberUrls.generateOne)
        (tracker add sameVersionInfoDone).unsafeRunSync() shouldBe true

        updateStatus(sameVersionInfoDone, Done)

        (tracker add subscriptionInfo).unsafeRunSync() shouldBe false

        findRows(sameVersionInfoDone.subscriberUrl) shouldBe List(
          (sameVersionInfoDone.subscriberUrl, sameVersionInfoDone.subscriberVersion, Done, ChangeDate(now), None)
        )
        findRows(subscriptionInfo.subscriberUrl) shouldBe Nil
      }
  }

  "remove" should {

    New :: Sent :: NonRecoverableFailure :: RecoverableFailure :: Nil foreach { status =>
      s"delete version -> url pair if exists and not in status $status" in new TestCase {

        (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

        updateStatus(subscriptionInfo, status)

        (tracker remove subscriptionInfo.subscriberUrl).unsafeRunSync() shouldBe true

        findRows(subscriptionInfo.subscriberUrl) shouldBe Nil
      }
    }

    "do not remove version -> url pairs with status DONE" in new TestCase {

      (tracker add subscriptionInfo).unsafeRunSync() shouldBe true
      updateStatus(subscriptionInfo, Done)

      val sameUrlDifferentVersion = subscriptionInfo.copy(subscriberVersion = serviceVersions.generateOne)
      (tracker add sameUrlDifferentVersion).unsafeRunSync() shouldBe true
      updateStatus(sameUrlDifferentVersion, Done)

      val sameUrlDifferentVersionNotDone1 = subscriptionInfo.copy(subscriberVersion = serviceVersions.generateOne)
      (tracker add sameUrlDifferentVersionNotDone1).unsafeRunSync() shouldBe true

      val sameUrlDifferentVersionNotDone2 = subscriptionInfo.copy(subscriberVersion = serviceVersions.generateOne)
      (tracker add sameUrlDifferentVersionNotDone2).unsafeRunSync() shouldBe true

      (tracker remove subscriptionInfo.subscriberUrl).unsafeRunSync() shouldBe true

      findRows(subscriptionInfo.subscriberUrl) should contain theSameElementsAs List(
        (subscriptionInfo.subscriberUrl, subscriptionInfo.subscriberVersion, Done, ChangeDate(now), None),
        (subscriptionInfo.subscriberUrl, sameUrlDifferentVersion.subscriberVersion, Done, ChangeDate(now), None)
      )
    }

    "do nothing for a non-existing url" in new TestCase {
      (tracker remove subscriberUrls.generateOne).unsafeRunSync() shouldBe true
    }
  }

  private trait TestCase {
    val subscriptionInfo = migratorSubscriptionInfos.generateOne
    val now              = Instant.now().truncatedTo(MICROS)

    private val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val currentTime              = mockFunction[Instant]
    val tracker                  = new SubscriberTracker[IO](queriesExecTimes, currentTime)

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
      session.prepare(query).use(_.stream(url, 32).compile.toList)
    }
  }

  private def updateStatus(info: MigratorSubscriptionInfo, status: MigrationStatus): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[MigrationStatus ~ SubscriberUrl ~ ServiceVersion] =
        sql"""UPDATE ts_migration
              SET status = $migrationStatusEncoder
              WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder
          """.command
      session
        .prepare(query)
        .use(_.execute(status ~ info.subscriberUrl ~ info.subscriberVersion))
        .void
    }
  }
}
