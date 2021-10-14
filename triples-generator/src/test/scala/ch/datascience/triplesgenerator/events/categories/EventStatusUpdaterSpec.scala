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

package ch.datascience.triplesgenerator.events.categories

import cats.effect.{BracketThrow, IO, Sync}
import cats.syntax.all._
import ch.datascience.compression.Zip
import ch.datascience.data.ErrorMessage
import ch.datascience.events.EventRequestContent
import ch.datascience.events.producers.EventSender
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.http.client.RestClient._
import ch.datascience.tinytypes.ByteArrayTinyType
import ch.datascience.tinytypes.contenttypes.ZippedContent
import ch.datascience.tinytypes.json.TinyTypeEncoders
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater._
import io.circe.literal._
import io.renku.jsonld.generators.JsonLDGenerators.jsonLDEntities
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventStatusUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers with TinyTypeEncoders {

  "toTriplesGenerated" should {

    "send a ToTriplesGenerated status change event" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne

      val jsonLDPayload = jsonLDEntities.generateOne
      val zippedPayload = zippedEventPayloads.generateOne
      (zip
        .zip[IO](_: String)(_: BracketThrow[IO], _: Sync[IO]))
        .expects(jsonLDPayload.toJson.noSpaces, *, *)
        .returning(zippedPayload.value.pure[IO])

      (eventSender
        .sendEvent(_: EventRequestContent.WithPayload[ByteArrayTinyType with ZippedContent], _: String)(
          _: PartEncoder[ByteArrayTinyType with ZippedContent]
        ))
        .expects(
          EventRequestContent.WithPayload[ByteArrayTinyType with ZippedContent](
            event = json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id": ${eventId.id.value},
              "project": {
                "id": ${eventId.projectId.value},
                "path": ${projectPath.value}
              },
              "newStatus": "TRIPLES_GENERATED", 
              "processingTime": ${processingTime.value}
            }""",
            payload = zippedPayload
          ),
          s"$categoryName: Change event status as $TriplesGenerated failed",
          ZipPartEncoder
        )
        .returning(IO.unit)

      updater
        .toTriplesGenerated(eventId, projectPath, jsonLDPayload, processingTime)
        .unsafeRunSync() shouldBe ()
    }
  }

  "toTriplesStore" should {

    s"send a ToTriplesStore status change event" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: String))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id": ${eventId.id.value},
              "project": {
                "id": ${eventId.projectId.value},
                "path": ${projectPath.value}
              },
              "newStatus": "TRIPLES_STORE", 
              "processingTime": ${processingTime.value}
            }"""
          ),
          s"$categoryName: Change event status as $TriplesStore failed"
        )
        .returning(IO.unit)

      updater
        .toTriplesStore(eventId, projectPath, processingTime)
        .unsafeRunSync() shouldBe ()
    }
  }

  "rollback" should {

    s"send a ToNew status change event" in new TestCase {
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: String))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id":           ${eventId.id.value},
              "project": {
                "id":   ${eventId.projectId.value},
                "path": ${projectPath.value}
              },
              "newStatus": ${New.value}
            }"""
          ),
          s"$categoryName: Change event status as $New failed"
        )
        .returning(IO.unit)

      updater.rollback[New](eventId, projectPath).unsafeRunSync() shouldBe ()
    }

    s"send a ToTriplesGenerated status change event" in new TestCase {
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: String))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id":           ${eventId.id},
              "project": {
                "id":   ${eventId.projectId},
                "path": $projectPath
              },
              "newStatus": $TriplesGenerated
            }"""
          ),
          s"$categoryName: Change event status as $TriplesGenerated failed"
        )
        .returning(IO.unit)

      updater.rollback[TriplesGenerated](eventId, projectPath).unsafeRunSync() shouldBe ()
    }
  }

  "toFailure" should {

    GenerationRecoverableFailure +: GenerationNonRecoverableFailure +: TransformationRecoverableFailure +: TransformationNonRecoverableFailure +: Nil foreach {
      eventStatus =>
        s"send a To$eventStatus status change event " in new TestCase {
          val exception = exceptions.generateOne
          (eventSender
            .sendEvent(_: EventRequestContent.NoPayload, _: String))
            .expects(
              EventRequestContent.NoPayload(
                json"""{
                  "categoryName": "EVENTS_STATUS_CHANGE",
                  "id":           ${eventId.id},
                  "project": {
                    "id":   ${eventId.projectId},
                    "path": $projectPath
                  },
                  "message":   ${ErrorMessage.withStackTrace(exception).value},  
                  "newStatus": $eventStatus 
                }"""
              ),
              s"$categoryName: Change event status as $eventStatus failed"
            )
            .returning(IO.unit)

          updater.toFailure(eventId, projectPath, eventStatus, exception).unsafeRunSync() shouldBe ()
        }
    }
  }

  private trait TestCase {
    val eventId     = compoundEventIds.generateOne
    val projectPath = projectPaths.generateOne

    val categoryName: events.CategoryName = categoryNames.generateOne
    val eventSender = mock[EventSender[IO]]
    val zip         = mock[Zip]
    val updater     = new EventStatusUpdaterImpl[IO](eventSender, categoryName, zip)
  }
}
