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

package io.renku.eventlog.events.consumers
package creation

import cats.effect.IO
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._
import io.renku.eventlog.events.consumers.creation.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog.events.consumers.creation.EventPersister.Result
import io.renku.events.EventRequestContent
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.blankStrings
import io.renku.graph.model.events.EventStatus
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
    with TableDrivenPropertyChecks
    with should.Matchers {

  "createHandlingDefinition.decode" should {
    "decode a valid event successfully" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Generators.newOrSkippedEvents.generateOne
      definition.decode(EventRequestContent(eventData.asJson)) shouldBe Right(eventData)
    }

    "fail on invalid event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Json.obj("invalid" -> true.asJson)
      definition.decode(EventRequestContent(eventData)).isLeft shouldBe true
    }

    "fail if the skipped event does not contain a message" in new TestCase {
      val eventData =
        Generators.skippedEvents.generateOne.asJson.deepMerge(json""" {"message":${blankStrings().generateOne} }""")
      val definition = handler.createHandlingDefinition()
      definition.decode(EventRequestContent(eventData.asJson)).isLeft shouldBe true
    }

    unacceptableStatuses.foreach { unacceptableStatus =>
      s"fail if the event status is $unacceptableStatus" in new TestCase {
        val eventData =
          Generators.newOrSkippedEvents.generateOne.asJson deepMerge json"""{"status": ${unacceptableStatus.value}}"""
        val definition = handler.createHandlingDefinition()
        definition.decode(EventRequestContent(eventData.asJson)).isLeft shouldBe true
      }
    }
  }

  "createHandlingDefinition.process" should {
    "call to EventPersister" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (eventPersister.storeNewEvent _).expects(*).returning(IO.pure(Result.Existed))
      definition.process(Generators.newOrSkippedEvents.generateOne).unsafeRunSync() shouldBe ()
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

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventPersister = mock[EventPersister[IO]]
    val handler        = new EventHandler[IO](eventPersister)
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
