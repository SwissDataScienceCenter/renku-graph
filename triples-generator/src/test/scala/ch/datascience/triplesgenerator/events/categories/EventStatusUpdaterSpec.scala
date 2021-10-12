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

import cats.syntax.all._
import ch.datascience.data.ErrorMessage
import ch.datascience.events.EventRequestContent
import ch.datascience.events.producers.EventSender
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.tinytypes.json.TinyTypeEncoders
import ch.datascience.triplesgenerator.events.categories.EventStatusUpdater._
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class EventStatusUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers with TinyTypeEncoders {

  "toTriplesGenerated" should {

    s"send a ToTriplesGenerated status change event" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne

      (eventSender.sendEvent _)
        .expects(
          EventRequestContent(
            json"""{
              "categoryName": "EVENTS_STATUS_CHANGE",
              "id": ${eventId.id.value},
              "project": {
                "id": ${eventId.projectId.value},
                "path": ${projectPath.value}
              },
              "newStatus": "TRIPLES_GENERATED", 
              "processingTime": ${processingTime.value}
            }""",
            json"""{"payload": ${rawTriples.value.noSpaces} , "schemaVersion": ${schemaVersion.value}}""".noSpaces.some
          ),
          s"$categoryName: Change event status as $TriplesGenerated failed"
        )
        .returning(().pure[Try])

      updater
        .toTriplesGenerated(eventId, projectPath, rawTriples, schemaVersion, processingTime) shouldBe ().pure[Try]

    }

  }

  "toTriplesStore" should {

    s"send a ToTriplesStore status change event" in new TestCase {
      val processingTime = eventProcessingTimes.generateOne

      (eventSender.sendEvent _)
        .expects(
          EventRequestContent(
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
        .returning(().pure[Try])

      updater
        .toTriplesStore(eventId, projectPath, processingTime) shouldBe ().pure[Try]

    }

  }

  "rollback" should {

    s"send a ToNew status change event" in new TestCase {
      (eventSender.sendEvent _)
        .expects(
          EventRequestContent(
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
        .returning(().pure[Try])

      updater.rollback[New](eventId, projectPath) shouldBe ().pure[Try]
    }

    s"send a ToTriplesGenerated status change event" in new TestCase {
      (eventSender.sendEvent _)
        .expects(
          EventRequestContent(
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
        .returning(().pure[Try])

      updater.rollback[TriplesGenerated](eventId, projectPath) shouldBe ().pure[Try]
    }

  }

  "toFailure" should {

    GenerationRecoverableFailure +: GenerationNonRecoverableFailure +: TransformationRecoverableFailure +: TransformationNonRecoverableFailure +: Nil foreach {
      eventStatus =>
        s"send a To$eventStatus status change event " in new TestCase {
          val exception = exceptions.generateOne
          (eventSender.sendEvent _)
            .expects(
              EventRequestContent(
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
            .returning(().pure[Try])

          updater.toFailure(eventId, projectPath, eventStatus, exception) shouldBe ().pure[Try]
        }
    }
  }

  private trait TestCase {
    val eventSender = mock[EventSender[Try]]
    val categoryName: events.CategoryName = categoryNames.generateOne
    val updater = new EventStatusUpdaterImpl[Try](eventSender, categoryName)

    val eventId       = compoundEventIds.generateOne
    val projectPath   = projectPaths.generateOne
    val rawTriples    = jsonLDTriples.generateOne
    val schemaVersion = projectSchemaVersions.generateOne
  }

}
