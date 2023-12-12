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
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.TSMigtationTypeSerializers._
import io.renku.eventlog._
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.events.Generators.{subscriberIds, subscriberUrls}
import io.renku.events.Subscription.SubscriberUrl
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk._
import skunk.implicits._

import java.time.Instant
import java.time.temporal.ChronoUnit.MICROS

class MigrationSubscriberTrackerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {
  private val subscriber = migrationSubscribers.generateOne

  "add" should {

    "insert a new row into the ts_migration table " +
      "if there's no row for the version " +
      "and the version -> url pair does not exists" in testDBResource.use { implicit cfg =>
        (tracker add subscriber).asserting(_ shouldBe true) >>
          findRows(subscriber.url)
            .asserting(_ shouldBe List((subscriber.url, subscriber.version, New, ChangeDate(now), None)))
      }

    "do nothing if there's already a row for the version -> url pair" in testDBResource.use { implicit cfg =>
      for {
        _ <- (tracker add subscriber).asserting(_ shouldBe true)

        differentIdInfo = subscriber.copy(id = subscriberIds.generateOne)
        _ <- (tracker add differentIdInfo).asserting(_ shouldBe true)

        _ <- findRows(subscriber.url)
               .asserting(_ shouldBe List((subscriber.url, subscriber.version, New, ChangeDate(now), None)))
      } yield Succeeded
    }

    "insert a new row " +
      "if there's a different version for the same url" in testDBResource.use { implicit cfg =>
        for {
          _ <- (tracker add subscriber).asserting(_ shouldBe true)

          differentVersionInfo = subscriber.copy(version = serviceVersions.generateOne)
          _ <- (tracker add differentVersionInfo).asserting(_ shouldBe true)

          _ <- findRows(subscriber.url).asserting {
                 _ should contain theSameElementsAs List(
                   (subscriber.url, subscriber.version, New, ChangeDate(now), None),
                   (differentVersionInfo.url, differentVersionInfo.version, New, ChangeDate(now), None)
                 )
               }
        } yield Succeeded
      }

    "insert a new row " +
      "if there's a different url for the version already existing in the table " +
      "but all the other same version urls are not in DONE" in testDBResource.use { implicit cfg =>
        for {
          otherStatusesInfos <- (MigrationStatus.all - Done).toList.map { status =>
                                  val sameVersionInfo = subscriber.copy(url = subscriberUrls.generateOne)
                                  (tracker add sameVersionInfo).asserting(_ shouldBe true) >>
                                    updateStatus(sameVersionInfo, status)
                                      .as(status -> sameVersionInfo)
                                }.sequence

          _ <- (tracker add subscriber).asserting(_ shouldBe true)

          _ <- findRows(subscriber.url)
                 .asserting(_ shouldBe List((subscriber.url, subscriber.version, New, ChangeDate(now), None)))

          _ <- otherStatusesInfos.map { case (status, info) =>
                 findRows(info.url).asserting(_ shouldBe List((info.url, info.version, status, ChangeDate(now), None)))
               }.sequence
        } yield Succeeded
      }

    "do not add a new row " +
      "if there's at least one url for the version already in status DONE" in testDBResource.use { implicit cfg =>
        val sameVersionInfoDone = subscriber.copy(url = subscriberUrls.generateOne)
        for {
          _ <- (tracker add sameVersionInfoDone).asserting(_ shouldBe true)

          _ <- updateStatus(sameVersionInfoDone, Done)

          _ <- (tracker add subscriber).asserting(_ shouldBe false)

          _ <- findRows(sameVersionInfoDone.url).asserting(
                 _ shouldBe List((sameVersionInfoDone.url, sameVersionInfoDone.version, Done, ChangeDate(now), None))
               )
          _ <- findRows(subscriber.url).asserting(_ shouldBe Nil)
        } yield Succeeded
      }
  }

  "remove" should {

    "delete version -> url pair if exists and not in certain status" in testDBResource.use { implicit cfg =>
      (New :: Sent :: NonRecoverableFailure :: RecoverableFailure :: Nil)
        .map { status =>
          for {
            _ <- (tracker add subscriber).asserting(_ shouldBe true)

            _ <- updateStatus(subscriber, status)

            _ <- (tracker remove subscriber.url).asserting(_ shouldBe true)

            _ <- findRows(subscriber.url).asserting(_ shouldBe Nil)
          } yield Succeeded
        }
        .sequence
        .void
    }

    "do not remove version -> url pairs with status DONE" in testDBResource.use { implicit cfg =>
      for {
        _ <- (tracker add subscriber).asserting(_ shouldBe true)
        _ <- updateStatus(subscriber, Done)

        sameUrlDifferentVersion = subscriber.copy(version = serviceVersions.generateOne)
        _ <- (tracker add sameUrlDifferentVersion).asserting(_ shouldBe true)
        _ <- updateStatus(sameUrlDifferentVersion, Done)

        sameUrlDifferentVersionNotDone1 = subscriber.copy(version = serviceVersions.generateOne)
        _ <- (tracker add sameUrlDifferentVersionNotDone1).asserting(_ shouldBe true)

        sameUrlDifferentVersionNotDone2 = subscriber.copy(version = serviceVersions.generateOne)
        _ <- (tracker add sameUrlDifferentVersionNotDone2).asserting(_ shouldBe true)

        _ <- (tracker remove subscriber.url).asserting(_ shouldBe true)

        _ <- findRows(subscriber.url).asserting {
               _ should contain theSameElementsAs List(
                 (subscriber.url, subscriber.version, Done, ChangeDate(now), None),
                 (subscriber.url, sameUrlDifferentVersion.version, Done, ChangeDate(now), None)
               )
             }
      } yield Succeeded
    }

    "do nothing for a non-existing url" in testDBResource.use { implicit cfg =>
      (tracker remove subscriberUrls.generateOne).asserting(_ shouldBe true)
    }
  }

  private lazy val now = Instant.now().truncatedTo(MICROS)

  private def tracker(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new MigrationSubscriberTracker[IO](() => now)
  }

  private def findRows(url: SubscriberUrl)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[List[(SubscriberUrl, ServiceVersion, MigrationStatus, ChangeDate, Option[MigrationMessage])]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[SubscriberUrl,
                       (SubscriberUrl, ServiceVersion, MigrationStatus, ChangeDate, Option[MigrationMessage])
      ] = sql"""
         SELECT subscriber_url, subscriber_version, status, change_date, message
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

  private def updateStatus(info: MigrationSubscriber, status: MigrationStatus)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[MigrationStatus *: SubscriberUrl *: ServiceVersion *: EmptyTuple] = sql"""
        UPDATE ts_migration
        SET status = $migrationStatusEncoder
        WHERE subscriber_url = $subscriberUrlEncoder AND subscriber_version = $serviceVersionEncoder""".command
      session
        .prepare(query)
        .flatMap(_.execute(status *: info.url *: info.version *: EmptyTuple))
        .void
    }
}
