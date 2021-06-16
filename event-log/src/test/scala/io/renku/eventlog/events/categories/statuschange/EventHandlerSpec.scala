/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.categories
package statuschange

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.events.categories.statuschange.Generators._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class EventHandlerSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with TableDrivenPropertyChecks
    with Eventually {

  "handle" should {
    Seq(triplesGeneratedEvents.generateOne, tripleStoreEvents.generateOne).foreach { event =>
      s"decode event changing project events to ${event.getClass.getSimpleName} and pass it to the events updater" in new TestCase {

        (statusChanger.updateStatuses _).expects(event).returning(().pure[IO])

        handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

        eventually {
          logger.loggedOnly(Info(s"$categoryName: ${event.show} -> Processed"),
                            Info(s"$categoryName: ${event.show} -> $Accepted")
          )
        }

      }
    }

    s"log an error if the events updater fails" in new TestCase {
      val event     = Gen.oneOf(tripleStoreEvents, triplesGeneratedEvents).generateOne
      val exception = exceptions.generateOne
      (statusChanger.updateStatuses _).expects(event).returning(exception.raiseError[IO, Unit])

      handler.handle(requestContent(event.asJson)).unsafeRunSync() shouldBe Accepted

      eventually {
        logger.loggedOnly(Error(s"$categoryName: ${event.show} -> Failure", exception),
                          Info(s"$categoryName: ${event.show} -> $Accepted")
        )
      }

    }

    s"return $UnsupportedEventType if event is of wrong category" in new TestCase {

      handler.handle(requestContent(jsons.generateOne.asJson)).unsafeRunSync() shouldBe UnsupportedEventType

      logger.expectNoLogs()
    }

    s"return $BadRequest if event is malformed" in new TestCase {

      val request = requestContent {
        jsons.generateOne deepMerge json"""{
          "categoryName": ${categoryName.value}
        }"""
      }

      handler.handle(request).unsafeRunSync() shouldBe BadRequest

      logger.expectNoLogs()
    }

  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)
  private trait TestCase {
    val logger        = TestLogger[IO]()
    val statusChanger = mock[StatusChanger[IO]]
    val handler       = new EventHandler[IO](categoryName, statusChanger, logger)
  }

  private implicit def eventEncoder[E <: StatusChangeEvent]: Encoder[E] = Encoder.instance[E] {
    case StatusChangeEvent.TriplesGenerated(eventId, path) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_GENERATED"
    }"""
    case StatusChangeEvent.TriplesStore(eventId, path) =>
      json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id.value},
      "project": {
        "id":         ${eventId.projectId.value},
        "path":       ${path.value}
      },
      "newStatus":    "TRIPLES_STORE"
    }"""

  }
}
