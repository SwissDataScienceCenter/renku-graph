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

package io.renku.triplesgenerator.events.consumers

import cats.effect.{IO, Sync}
import cats.syntax.all._
import io.circe.literal._
import io.renku.compression.Zip
import io.renku.data.Message
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.http.client.RestClient._
import io.renku.testtools.IOSpec
import io.renku.tinytypes.ByteArrayTinyType
import io.renku.tinytypes.contenttypes.ZippedContent
import io.renku.triplesgenerator.events.consumers.EventStatusUpdater._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventStatusUpdaterSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "toTriplesGenerated" should {

    "send a ToTriplesGenerated status change event" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne

      val jsonLDPayload = jsonLDEntities.generateOne
      val zippedPayload = zippedEventPayloads.generateOne
      (zip
        .zip[IO](_: String)(_: Sync[IO]))
        .expects(jsonLDPayload.toJson.noSpaces, *)
        .returning(zippedPayload.value.pure[IO])

      (eventSender
        .sendEvent(_: EventRequestContent.WithPayload[ByteArrayTinyType with ZippedContent],
                   _: EventSender.EventContext
        )(_: PartEncoder[ByteArrayTinyType with ZippedContent]))
        .expects(
          EventRequestContent.WithPayload[ByteArrayTinyType with ZippedContent](
            event = json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id": ${eventId.id},
              "project": {
                "id":   ${eventId.projectId},
                "slug": $projectSlug
              },
              "subCategory":    "ToTriplesGenerated",
              "processingTime": $processingTime
            }""",
            payload = zippedPayload
          ),
          EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                   s"$categoryName: Change event status as $TriplesGenerated failed"
          ),
          ZipPartEncoder
        )
        .returning(IO.unit)

      updater
        .toTriplesGenerated(eventId, projectSlug, jsonLDPayload, processingTime)
        .unsafeRunSync() shouldBe ()
    }
  }

  "toTriplesStore" should {

    s"send a ToTriplesStore status change event" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne

      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id": ${eventId.id},
              "project": {
                "id":   ${eventId.projectId},
                "slug": $projectSlug
              },
              "subCategory":    "ToTriplesStore",
              "processingTime": $processingTime
            }"""
          ),
          EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                   s"$categoryName: Change event status as $TriplesStore failed"
          )
        )
        .returning(IO.unit)

      updater
        .toTriplesStore(eventId, projectSlug, processingTime)
        .unsafeRunSync() shouldBe ()
    }
  }

  "rollback" should {

    s"send a ToNew status change event" in new TestCase {
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id":           ${eventId.id},
              "project": {
                "id":   ${eventId.projectId},
                "slug": $projectSlug
              },
              "subCategory": "RollbackToNew"
            }"""
          ),
          EventSender.EventContext(
            CategoryName("EVENTS_STATUS_CHANGE"),
            s"$categoryName: Change event status as ${RollbackStatus.New} failed"
          )
        )
        .returning(IO.unit)

      updater.rollback(eventId, projectSlug, RollbackStatus.New).unsafeRunSync() shouldBe ()
    }

    s"send a ToTriplesGenerated status change event" in new TestCase {
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id":           ${eventId.id},
              "project": {
                "id":   ${eventId.projectId},
                "slug": $projectSlug
              },
              "subCategory": "RollbackToTriplesGenerated"
            }"""
          ),
          EventSender.EventContext(
            CategoryName("EVENTS_STATUS_CHANGE"),
            s"$categoryName: Change event status as ${RollbackStatus.TriplesGenerated} failed"
          )
        )
        .returning(IO.unit)

      updater.rollback(eventId, projectSlug, RollbackStatus.TriplesGenerated).unsafeRunSync() shouldBe ()
    }
  }

  "toFailure" should {

    GenerationRecoverableFailure +: GenerationNonRecoverableFailure +: TransformationRecoverableFailure +: TransformationNonRecoverableFailure +: Nil foreach {
      eventStatus =>
        s"send a To$eventStatus status change event " in new TestCase {
          val exception = exceptions.generateOne
          (eventSender
            .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
            .expects(
              EventRequestContent.NoPayload(
                json"""{
                  "categoryName": "EVENTS_STATUS_CHANGE",
                  "id":           ${eventId.id},
                  "project": {
                    "id":   ${eventId.projectId},
                    "slug": $projectSlug
                  },
                  "subCategory": "ToFailure",
                  "message":   ${Message.Error.fromStackTrace(exception).show},
                  "newStatus": $eventStatus
                }"""
              ),
              EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                       s"$categoryName: Change event status as $eventStatus failed"
              )
            )
            .returning(IO.unit)

          updater.toFailure(eventId, projectSlug, eventStatus, exception).unsafeRunSync() shouldBe ()
        }
    }
  }

  "projectToNew" should {

    s"send a projectToNew status change event" in new TestCase {
      (eventSender
        .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
        .expects(
          EventRequestContent.NoPayload(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "project": {
                "id":   ${eventId.projectId},
                "slug": $projectSlug
              },
              "subCategory": "ProjectEventsToNew"
            }"""
          ),
          EventSender.EventContext(CategoryName("EVENTS_STATUS_CHANGE"),
                                   s"$categoryName: Change project events status as $New failed"
          )
        )
        .returning(IO.unit)

      updater.projectToNew(Project(eventId.projectId, projectSlug)).unsafeRunSync() shouldBe ()
    }
  }
  private trait TestCase {
    val eventId     = compoundEventIds.generateOne
    val projectSlug = projectSlugs.generateOne

    val categoryName = categoryNames.generateOne
    val eventSender  = mock[EventSender[IO]]
    val zip          = mock[Zip]
    val updater      = new EventStatusUpdaterImpl[IO](eventSender, categoryName, zip)
  }
}
