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

package io.renku.eventlog.events.consumers
package creation

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.eventlog.events.consumers.creation.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog.events.consumers.creation.EventPersister.Result.{Created, Existed}
import io.renku.eventlog.events.consumers.creation.Generators._
import io.renku.events.Generators._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest, SchedulingError}
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.events.EventStatus
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with TableDrivenPropertyChecks
    with should.Matchers {

  "createHandlingProcess" should {

    val scenarios = Table(
      "status"  -> "resultFactory",
      "Created" -> ((event: Event) => Created(event)),
      "Existed" -> ((_: Event) => Existed)
    )
    forAll(scenarios) { case (status, resultFactory) =>
      s"return $Accepted if the creation of the event returns $status" in new TestCase {
        val event = newOrSkippedEvents.generateOne

        (eventPersister.storeNewEvent _).expects(event).returning(resultFactory(event).pure[IO])

        handler
          .createHandlingProcess(requestContent(event.asJson))
          .unsafeRunSync()
          .process
          .value
          .unsafeRunSync() shouldBe Right(Accepted)

        logger.loggedOnly(
          Info(
            s"$categoryName: ${event.compoundEventId}, projectPath = ${event.project.path}, status = ${event.status} -> $Accepted"
          )
        )
      }
    }

    s"return $BadRequest if the eventJson is malformed" in new TestCase {
      val event = jsons.generateOne deepMerge json""" {"categoryName":${categoryName.value} }"""

      val requestContent = eventRequestContentNoPayloads.generateOne.copy(event)

      handler.createHandlingProcess(requestContent).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(
        BadRequest
      )

      logger.expectNoLogs()
    }

    s"return $BadRequest if the skipped event does not contain a message" in new TestCase {
      val event = skippedEvents.generateOne.asJson.deepMerge(json""" {"message":${blankStrings().generateOne} }""")

      val requestContent = eventRequestContentNoPayloads.generateOne.copy(event)

      handler.createHandlingProcess(requestContent).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(
        BadRequest
      )

      logger.expectNoLogs()
    }

    unacceptableStatuses.foreach { unacceptableStatus =>
      s"return $BadRequest if the event status is $unacceptableStatus" in new TestCase {
        val event =
          newOrSkippedEvents.generateOne.asJson deepMerge json"""{"status": ${unacceptableStatus.value}}"""

        val requestContent = eventRequestContentNoPayloads.generateOne.copy(event)

        handler.createHandlingProcess(requestContent).unsafeRunSync().process.value.unsafeRunSync() shouldBe Left(
          BadRequest
        )

        logger.expectNoLogs()
      }
    }

    s"return $SchedulingError if the persister fails" in new TestCase {
      val event     = newOrSkippedEvents.generateOne
      val exception = exceptions.generateOne

      (eventPersister.storeNewEvent _).expects(event).returning(exception.raiseError[IO, EventPersister.Result])

      handler
        .createHandlingProcess(requestContent(event.asJson))
        .unsafeRunSync()
        .process
        .value
        .unsafeRunSync() shouldBe Left(SchedulingError(exception))

      logger.loggedOnly(
        Error(
          s"$categoryName: ${event.compoundEventId}, projectPath = ${event.project.path}, status = ${event.status} -> $SchedulingError",
          exception
        )
      )
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventPersister = mock[EventPersister[IO]]
    val handler        = new EventHandler[IO](categoryName, eventPersister)
  }

  private def toJson(event: Event): Json =
    json"""{
      "categoryName": ${categoryName.value},
      "id":         ${event.id.value},
      "project":    ${event.project},
      "date":       ${event.date.value},
      "batchDate":  ${event.batchDate.value},
      "body":       ${event.body.value},
      "status":     ${event.status.value}
    }"""

  private implicit lazy val projectEncoder: Encoder[Project] = Encoder.instance[Project] { project =>
    json"""{
      "id":   ${project.id.value},
      "path": ${project.path.value}
    }"""
  }

  private lazy val unacceptableStatuses = EventStatus.all.diff(Set(EventStatus.New, EventStatus.Skipped))

  private implicit def eventEncoder[T <: Event]: Encoder[T] = Encoder.instance[T] {
    case event: NewEvent     => toJson(event)
    case event: SkippedEvent => toJson(event) deepMerge json"""{ "message":    ${event.message.value} }"""
  }
}
