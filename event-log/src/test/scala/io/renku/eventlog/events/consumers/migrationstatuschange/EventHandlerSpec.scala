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

package io.renku.eventlog.events.consumers.migrationstatuschange

import cats.effect.IO
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.{MigrationMessage, MigrationStatus}
import io.renku.events.EventRequestContent
import io.renku.events.Generators.subscriberUrls
import io.renku.generators.CommonGraphGenerators.{microserviceIdentifiers, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
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

  "createHandlingDefinition.decode" should {
    List(Done, NonRecoverableFailure, RecoverableFailure).foreach { status =>
      s"decode a valid event successfully for status $status" in new TestCase {
        val definition = handler.createHandlingDefinition()
        val eventData  = generatePayload(status)
        val event      = createEvent(status)
        definition.decode(eventData) shouldBe Right(event)
      }
    }

    "fail on invalid event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Json.obj("invalid" -> true.asJson)
      definition.decode(EventRequestContent(eventData)).isLeft shouldBe true
    }
  }

  "createHandlingDefinition.process" should {
    "call to StatusUpdater" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (statusUpdater.updateStatus _).expects(*).returning(IO.unit)
      definition.process(Event.ToDone(url, version)).unsafeRunSync() shouldBe ()
    }
  }

  "createHandlingDefinition" should {
    "not define onRelease and precondition" in new TestCase {
      val definition = handler.createHandlingDefinition()
      definition.onRelease                    shouldBe None
      definition.precondition.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {

    val url     = subscriberUrls.generateOne
    val version = serviceVersions.generateOne
    val message = sentences().map(_.value).generateAs(MigrationMessage)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val statusUpdater = mock[StatusUpdater[IO]]
    val handler       = new EventHandler[IO](statusUpdater)

    def createEvent(status: MigrationStatus): Event =
      status match {
        case Done                  => Event.ToDone(url, version)
        case NonRecoverableFailure => Event.ToNonRecoverableFailure(url, version, message)
        case RecoverableFailure    => Event.ToRecoverableFailure(url, version, message)
        case _                     => sys.error(s"Invalid status for event: $status")
      }

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
