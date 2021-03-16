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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits.catsSyntaxOptionId
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.EventRequestContent
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.http.ErrorMessage
import ch.datascience.http.client.IORestClient
import ch.datascience.rdfstore.JsonLDTriples
import io.chrisdavenport.log4cats.Logger
import org.http4s.circe.jsonEncoder
import org.http4s.{Status, Uri}

import scala.concurrent.ExecutionContext

private trait EventStatusUpdater[Interpretation[_]] {
  def markEventNew(eventId:                     CompoundEventId): Interpretation[Unit]
  def markTriplesStore(eventId:                 CompoundEventId, processingTime: EventProcessingTime): Interpretation[Unit]
  def markTriplesGenerated(eventId:             CompoundEventId,
                           payload:             JsonLDTriples,
                           schemaVersion:       SchemaVersion,
                           maybeProcessingTime: Option[EventProcessingTime]
  ): Interpretation[Unit]
  def markEventFailed(eventId: CompoundEventId, eventStatus: EventStatus, exception: Throwable): Interpretation[Unit]
}

private class EventStatusUpdaterImpl(
    eventLogUrl: EventLogUrl,
    logger:      Logger[IO]
)(implicit
    ME:               MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with EventStatusUpdater[IO] {

  import cats.effect._
  import io.circe._
  import io.circe.literal._
  import org.http4s.Method.PATCH
  import org.http4s.Status.{Conflict, NotFound, Ok}
  import org.http4s.{Request, Response}

  override def markEventNew(eventId: CompoundEventId): IO[Unit] = sendStatusChange(
    eventId,
    eventContent = EventRequestContent(json"""{"status": ${EventStatus.New.value}}"""),
    responseMapping = okConflictAsSuccess
  )

  override def markTriplesStore(eventId: CompoundEventId, processingTime: EventProcessingTime): IO[Unit] =
    sendStatusChange(
      eventId,
      eventContent = EventRequestContent(
        event = json"""{
          "status":         ${EventStatus.TriplesStore.value}, 
          "processingTime": $processingTime
        }"""
      ),
      responseMapping = okConflictAsSuccess
    )

  override def markTriplesGenerated(eventId:             CompoundEventId,
                                    payload:             JsonLDTriples,
                                    schemaVersion:       SchemaVersion,
                                    maybeProcessingTime: Option[EventProcessingTime]
  ): IO[Unit] = sendStatusChange(
    eventId,
    eventContent = EventRequestContent(
      event = json"""{
        "status": ${EventStatus.TriplesGenerated.value}
      }""" deepMerge maybeProcessingTime
        .map(time => json"""{"processingTime": $time}""")
        .getOrElse(Json.obj()),
      maybePayload = json"""{
        "payload": ${payload.value.noSpaces},
        "schemaVersion": ${schemaVersion.value}
      }""".noSpaces.some
    ),
    responseMapping = okConflictAsSuccess
  )

  override def markEventFailed(eventId: CompoundEventId, eventStatus: EventStatus, exception: Throwable): IO[Unit] =
    sendStatusChange(
      eventId,
      eventContent = EventRequestContent(
        json"""{
          "status":  ${eventStatus.value},
          "message": ${ErrorMessage(exception).value}
        }"""
      ),
      responseMapping = okConflictAsSuccess
    )

  private def sendStatusChange(
      eventId:         CompoundEventId,
      eventContent:    EventRequestContent,
      responseMapping: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]]
  ): IO[Unit] = for {
    uri <- validateUri(s"$eventLogUrl/events/${eventId.id}/${eventId.projectId}")
    request = createRequest(uri, eventContent)
    sendingResult <- send(request)(responseMapping)
  } yield sendingResult

  private def createRequest(uri: Uri, eventRequestContent: EventRequestContent) =
    eventRequestContent.maybePayload match {
      case Some(payload) =>
        request(PATCH, uri).withMultipartBuilder
          .addPart("event", eventRequestContent.event)
          .addPart("payload", payload)
          .build()
      case None =>
        request(PATCH, uri).withEntity(eventRequestContent.event)
    }

  private lazy val okConflictAsSuccess: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Ok, _, _)       => IO.unit
    case (Conflict, _, _) => IO.unit
    case (NotFound, _, _) => IO.unit
  }
}

private object IOEventStatusUpdater {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventStatusUpdater[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventStatusUpdaterImpl(eventLogUrl, logger)
}
