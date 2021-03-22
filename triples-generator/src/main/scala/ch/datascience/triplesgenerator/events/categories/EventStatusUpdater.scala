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

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{Eval, MonadError}
import ch.datascience.control.Throttler
import ch.datascience.data.ErrorMessage
import ch.datascience.events.consumers.EventRequestContent
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.http.client.IORestClient
import ch.datascience.http.client.RestClientError.{ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.JsonLDTriples
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status.{BadGateway, GatewayTimeout, ServiceUnavailable}
import org.http4s.{Status, Uri}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

private trait EventStatusUpdater[Interpretation[_]] {
  def markTriplesGenerated(eventId:        CompoundEventId,
                           payload:        JsonLDTriples,
                           schemaVersion:  SchemaVersion,
                           processingTime: EventProcessingTime
  ): Interpretation[Unit]
  def markTriplesStore(eventId:     CompoundEventId, processingTime: EventProcessingTime): Interpretation[Unit]
  def markEventNew(eventId:         CompoundEventId): Interpretation[Unit]
  def markTriplesGenerated(eventId: CompoundEventId): Interpretation[Unit]
  def markEventFailed(eventId:      CompoundEventId, eventStatus: EventStatus, exception: Throwable): Interpretation[Unit]
}

private class EventStatusUpdaterImpl(
    eventLogUrl:  EventLogUrl,
    categoryName: CategoryName,
    retryDelay:   FiniteDuration,
    logger:       Logger[IO]
)(implicit
    ME:               MonadError[IO, Throwable],
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(Throttler.noThrottling, logger)
    with EventStatusUpdater[IO] {

  import cats.effect._
  import io.circe.literal._
  import org.http4s.Method.PATCH
  import org.http4s.Status.{NotFound, Ok}
  import org.http4s.{Request, Response}

  override def markEventNew(eventId: CompoundEventId): IO[Unit] = sendStatusChange(
    eventId,
    eventContent = EventRequestContent(json"""{"status": ${EventStatus.New.value}}"""),
    responseMapping
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
      responseMapping
    )

  override def markTriplesGenerated(eventId:        CompoundEventId,
                                    payload:        JsonLDTriples,
                                    schemaVersion:  SchemaVersion,
                                    processingTime: EventProcessingTime
  ): IO[Unit] = sendStatusChange(
    eventId,
    eventContent = EventRequestContent(
      event = json"""{
        "status": ${EventStatus.TriplesGenerated.value},
        "processingTime": $processingTime
      }""",
      maybePayload = json"""{
        "payload": ${payload.value.noSpaces},
        "schemaVersion": ${schemaVersion.value}
      }""".noSpaces.some
    ),
    responseMapping
  )

  override def markTriplesGenerated(eventId: CompoundEventId): IO[Unit] =
    sendStatusChange(
      eventId,
      eventContent = EventRequestContent(
        event = json"""{"status": ${EventStatus.TriplesGenerated.value}}"""
      ),
      responseMapping
    )

  override def markEventFailed(eventId: CompoundEventId, eventStatus: EventStatus, exception: Throwable): IO[Unit] =
    sendStatusChange(
      eventId,
      eventContent = EventRequestContent(
        json"""{
          "status":  ${eventStatus.value},
          "message": ${ErrorMessage.withStackTrace(exception).value}
        }"""
      ),
      responseMapping
    )

  private def sendStatusChange(
      eventId:         CompoundEventId,
      eventContent:    EventRequestContent,
      responseMapping: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]]
  ): IO[Unit] = for {
    uri <- validateUri(s"$eventLogUrl/events/${eventId.id}/${eventId.projectId}")
    request = createRequest(uri, eventContent)
    sendingResult <- send(request)(responseMapping) recoverWith retryOnServerError(
                       Eval.always(sendStatusChange(eventId, eventContent, responseMapping))
                     )
  } yield sendingResult

  def retryOnServerError(retry: Eval[IO[Unit]]): PartialFunction[Throwable, IO[Unit]] = {
    case UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, message) =>
      waitAndRetry(retry, message)
    case ConnectivityException(message, _) => waitAndRetry(retry, message)
  }

  private def waitAndRetry(retry: Eval[IO[Unit]], errorMessage: String) = for {
    _      <- logger.error(s"$categoryName: sending status change failed - retrying in $retryDelay - $errorMessage")
    _      <- timer sleep retryDelay
    result <- retry.value
  } yield result

  private def createRequest(uri: Uri, eventRequestContent: EventRequestContent) =
    request(PATCH, uri).withMultipartBuilder
      .addPart("event", eventRequestContent.event)
      .maybeAddPart("payload", eventRequestContent.maybePayload)
      .build()

  private lazy val responseMapping: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Ok, _, _)       => IO.unit
    case (NotFound, _, _) => IO.unit
  }
}

private object IOEventStatusUpdater {
  def apply(
      categoryName: CategoryName,
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventStatusUpdater[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventStatusUpdaterImpl(eventLogUrl, categoryName, retryDelay = 30 seconds, logger)
}
