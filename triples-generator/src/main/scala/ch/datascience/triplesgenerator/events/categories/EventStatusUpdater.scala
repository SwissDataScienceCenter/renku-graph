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

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.data.ErrorMessage
import ch.datascience.events
import ch.datascience.events.EventRequestContent
import ch.datascience.events.producers.EventSender
import ch.datascience.graph.model.events.EventStatus.{FailureStatus, TriplesGenerated, TriplesStore}
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.json.TinyTypeEncoders
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait EventStatusUpdater[Interpretation[_]] {
  def toTriplesGenerated(eventId:        CompoundEventId,
                         projectPath:    projects.Path,
                         payload:        JsonLDTriples,
                         schemaVersion:  SchemaVersion,
                         processingTime: EventProcessingTime
  ): Interpretation[Unit]

  def toTriplesStore(eventId:        CompoundEventId,
                     projectPath:    projects.Path,
                     processingTime: EventProcessingTime
  ): Interpretation[Unit]

  def rollback[S <: EventStatus](eventId: CompoundEventId, projectPath: projects.Path)(implicit
      rollbackStatus:                     () => S
  ): Interpretation[Unit]

  def toFailure(eventId:     CompoundEventId,
                projectPath: projects.Path,
                eventStatus: FailureStatus,
                exception:   Throwable
  ): Interpretation[Unit]
}

private class EventStatusUpdaterImpl[Interpretation[_]: MonadThrow](
    eventSender:  EventSender[Interpretation],
    categoryName: CategoryName
) extends EventStatusUpdater[Interpretation]
    with TinyTypeEncoders {

  import io.circe.literal._

  override def toTriplesGenerated(eventId:        CompoundEventId,
                                  projectPath:    projects.Path,
                                  payload:        JsonLDTriples,
                                  schemaVersion:  SchemaVersion,
                                  processingTime: EventProcessingTime
  ): Interpretation[Unit] = eventSender.sendEvent(
    eventContent = events.EventRequestContent(
      event = json"""{
        "categoryName": "EVENTS_STATUS_CHANGE",
        "id": ${eventId.id},
        "project": {
          "id":   ${eventId.projectId},
          "path": $projectPath
        },
        "newStatus": $TriplesGenerated,
        "processingTime": $processingTime
      }""",
      maybePayload = json"""{
        "payload": ${payload.value.noSpaces},
        "schemaVersion": $schemaVersion
      }""".noSpaces.some
    ),
    errorMessage = s"$categoryName: Change event status as $TriplesGenerated failed"
  )

  override def toTriplesStore(eventId:        CompoundEventId,
                              projectPath:    projects.Path,
                              processingTime: EventProcessingTime
  ): Interpretation[Unit] = eventSender.sendEvent(
    eventContent = EventRequestContent(json"""{ 
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id": ${eventId.id},
      "project": {
        "id":   ${eventId.projectId},
        "path": $projectPath
      },
      "newStatus":      $TriplesStore, 
      "processingTime": $processingTime
    }"""),
    errorMessage = s"$categoryName: Change event status as $TriplesStore failed"
  )

  override def rollback[S <: EventStatus](
      eventId:               CompoundEventId,
      projectPath:           projects.Path
  )(implicit rollbackStatus: () => S): Interpretation[Unit] = eventSender.sendEvent(
    eventContent = EventRequestContent(json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id},
      "project": {
        "id":   ${eventId.projectId},
        "path": $projectPath
      },
      "newStatus": ${rollbackStatus().value}
    }"""),
    errorMessage = s"$categoryName: Change event status as ${rollbackStatus().value} failed"
  )

  override def toFailure(eventId:     CompoundEventId,
                         projectPath: projects.Path,
                         eventStatus: FailureStatus,
                         exception:   Throwable
  ): Interpretation[Unit] = eventSender.sendEvent(
    eventContent = EventRequestContent(json"""{
      "categoryName": "EVENTS_STATUS_CHANGE",
      "id":           ${eventId.id},
      "project": {
        "id":   ${eventId.projectId},
        "path": $projectPath
      },
      "newStatus": $eventStatus,
      "message":   ${ErrorMessage.withStackTrace(exception).value}
    }"""),
    errorMessage = s"$categoryName: Change event status as $eventStatus failed"
  )
}

private object EventStatusUpdater {

  implicit val rollbackToNew:              () => EventStatus.New              = () => EventStatus.New
  implicit val rollbackToTriplesGenerated: () => EventStatus.TriplesGenerated = () => EventStatus.TriplesGenerated

  def apply(
      categoryName: CategoryName,
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventStatusUpdater[IO]] = for {
    eventSender <- EventSender(logger)
  } yield new EventStatusUpdaterImpl(eventSender, categoryName)
}
