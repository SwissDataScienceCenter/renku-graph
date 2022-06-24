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
import io.circe.literal._
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.events.consumers.migrationstatuschange.Event._
import io.renku.eventlog.{MigrationMessage, MigrationStatus}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest, SchedulingError}
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.CommonGraphGenerators.{microserviceIdentifiers, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with TableDrivenPropertyChecks {

  "tryHandling" should {

    "decode an event from the request, " +
      "pass it to the Status Updater " +
      s"and return $Accepted" in new TestCase {
        forAll {
          Table(
            ("Event", "Json"),
            (ToDone(url, version), generatePayload(Done)),
            (ToRecoverableFailure(url, version, message), generatePayload(RecoverableFailure)),
            (ToNonRecoverableFailure(url, version, message), generatePayload(NonRecoverableFailure))
          )
        } { (event, eventJson) =>
          (statusUpdater.updateStatus _).expects(event).returning(().pure[IO])

          handler.tryHandling(eventJson).unsafeRunSync() shouldBe Accepted

          logger.loggedOnly(Info(show"$categoryName: $event -> $Accepted"))

          logger.reset()
        }
      }

    MigrationStatus.all - Done - NonRecoverableFailure - RecoverableFailure foreach { status =>
      s"fail with $BadRequest for event with newStatus = $status" in new TestCase {

        handler.tryHandling(generatePayload(status)).unsafeRunSync() shouldBe BadRequest

        logger.expectNoLogs()
      }
    }

    NonRecoverableFailure :: RecoverableFailure :: Nil foreach { status =>
      s"fail with $BadRequest for event with newStatus = $status and no message" in new TestCase {

        handler
          .tryHandling(EventRequestContent.NoPayload(json"""{
            "categoryName": "MIGRATION_STATUS_CHANGE",
            "subscriber": {
              "url":     ${url.value},
              "id":      ${microserviceIdentifiers.generateOne.value},
              "version": ${version.value}
            },
            "newStatus": ${status.value}
          }"""))
          .unsafeRunSync() shouldBe BadRequest

        logger.expectNoLogs()
      }
    }

    "log an error if the events updater fails" in new TestCase {

      val event     = ToDone(url, version)
      val exception = exceptions.generateOne
      (statusUpdater.updateStatus _)
        .expects(event)
        .returning(exception.raiseError[IO, Unit])

      handler.tryHandling(generatePayload(Done)).unsafeRunSync() shouldBe SchedulingError(exception)

      logger.loggedOnly(Error(show"$categoryName: $event -> SchedulingError", exception))
    }
  }

  private trait TestCase {

    val url     = subscriberUrls.generateOne
    val version = serviceVersions.generateOne
    val message = sentences().map(_.value).generateAs(MigrationMessage)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val statusUpdater = mock[StatusUpdater[IO]]
    val handler       = new EventHandler[IO](statusUpdater)

    def generatePayload: MigrationStatus => EventRequestContent = {
      case Done => EventRequestContent.NoPayload(json"""{
        "categoryName": "MIGRATION_STATUS_CHANGE",
        "subscriber": {
          "url":     ${url.value},
          "id":      ${microserviceIdentifiers.generateOne.value},
          "version": ${version.value}
        },
        "newStatus": ${Done.value}
      }""")
      case NonRecoverableFailure => EventRequestContent.NoPayload(json"""{
        "categoryName": "MIGRATION_STATUS_CHANGE",
        "subscriber": {
          "url":     ${url.value},
          "id":      ${microserviceIdentifiers.generateOne.value},
          "version": ${version.value}
        },
        "newStatus": ${NonRecoverableFailure.value},
        "message":   ${message.value}
      }""")
      case RecoverableFailure => EventRequestContent.NoPayload(json"""{
        "categoryName": "MIGRATION_STATUS_CHANGE",
        "subscriber": {
          "url":     ${url.value},
          "id":      ${microserviceIdentifiers.generateOne.value},
          "version": ${version.value}
        },
        "newStatus": ${RecoverableFailure.value},
        "message":   ${message.value}
      }""")
      case other => EventRequestContent.NoPayload(json"""{
        "categoryName": "MIGRATION_STATUS_CHANGE",
        "subscriber": {
          "url":     ${url.value},
          "id":      ${microserviceIdentifiers.generateOne.value},
          "version": ${version.value}
        },
        "newStatus": ${other.value}
      }""")
    }
  }
}
