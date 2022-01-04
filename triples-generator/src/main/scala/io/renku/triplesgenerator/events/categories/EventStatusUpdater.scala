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

package io.renku.triplesgenerator.events.categories

import cats.effect.{Async, Sync}
import cats.syntax.all._
import io.renku.compression.Zip
import io.renku.data.ErrorMessage
import io.renku.events
import io.renku.events.EventRequestContent
import io.renku.events.producers.EventSender
import io.renku.graph.model.events.EventStatus.{FailureStatus, TriplesGenerated, TriplesStore}
import io.renku.graph.model.events.{CategoryName, CompoundEventId, EventProcessingTime, EventStatus, ZippedEventPayload}
import io.renku.graph.model.projects
import io.renku.jsonld.JsonLD
import io.renku.tinytypes.json.TinyTypeEncoders
import org.typelevel.log4cats.Logger

private trait EventStatusUpdater[F[_]] {
  def toTriplesGenerated(eventId:        CompoundEventId,
                         projectPath:    projects.Path,
                         payload:        JsonLD,
                         processingTime: EventProcessingTime
  ): F[Unit]

  def toTriplesStore(eventId: CompoundEventId, projectPath: projects.Path, processingTime: EventProcessingTime): F[Unit]

  def rollback[S <: EventStatus](eventId: CompoundEventId, projectPath: projects.Path)(implicit
      rollbackStatus:                     () => S
  ): F[Unit]

  def toFailure(eventId:     CompoundEventId,
                projectPath: projects.Path,
                eventStatus: FailureStatus,
                exception:   Throwable
  ): F[Unit]
}

private class EventStatusUpdaterImpl[F[_]: Sync](
    eventSender:  EventSender[F],
    categoryName: CategoryName,
    zipper:       Zip
) extends EventStatusUpdater[F]
    with TinyTypeEncoders {

  import io.circe.literal._
  import zipper._

  override def toTriplesGenerated(eventId:        CompoundEventId,
                                  projectPath:    projects.Path,
                                  payload:        JsonLD,
                                  processingTime: EventProcessingTime
  ): F[Unit] = for {
    zippedContent <- zip(payload.toJson.noSpaces).map(ZippedEventPayload.apply)
    _ <- eventSender.sendEvent(
           eventContent = events.EventRequestContent.WithPayload(
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
             payload = zippedContent
           ),
           errorMessage = s"$categoryName: Change event status as $TriplesGenerated failed"
         )
  } yield ()

  override def toTriplesStore(eventId:        CompoundEventId,
                              projectPath:    projects.Path,
                              processingTime: EventProcessingTime
  ): F[Unit] = eventSender.sendEvent(
    eventContent = EventRequestContent.NoPayload(json"""{ 
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
  )(implicit rollbackStatus: () => S): F[Unit] = eventSender.sendEvent(
    eventContent = EventRequestContent.NoPayload(json"""{
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
  ): F[Unit] = eventSender.sendEvent(
    eventContent = EventRequestContent.NoPayload(json"""{
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

  def apply[F[_]: Async: Logger](categoryName: CategoryName): F[EventStatusUpdater[F]] = for {
    eventSender <- EventSender[F]
  } yield new EventStatusUpdaterImpl(eventSender, categoryName, Zip)
}
