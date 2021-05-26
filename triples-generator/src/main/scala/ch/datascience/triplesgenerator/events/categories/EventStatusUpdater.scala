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

import cats.Eval
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.data.ErrorMessage
import ch.datascience.events.consumers.EventRequestContent
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventStatus.FailureStatus
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.http.client.RestClient
import ch.datascience.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.JsonLDTriples
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import org.http4s.Status.{BadGateway, GatewayTimeout, ServiceUnavailable}
import org.http4s.{Status, Uri}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

private trait EventStatusUpdater[Interpretation[_]] {
  def toTriplesGenerated(eventId:        CompoundEventId,
                         payload:        JsonLDTriples,
                         schemaVersion:  SchemaVersion,
                         processingTime: EventProcessingTime
  ): Interpretation[Unit]

  def toTriplesStore(eventId: CompoundEventId, processingTime: EventProcessingTime): Interpretation[Unit]

  def rollback[S <: EventStatus](eventId: CompoundEventId)(implicit rollbackStatus: () => S): Interpretation[Unit]

  def toFailure(eventId: CompoundEventId, eventStatus: FailureStatus, exception: Throwable): Interpretation[Unit]
}

private class EventStatusUpdaterImpl[Interpretation[_]: ConcurrentEffect: Timer](
    eventLogUrl:             EventLogUrl,
    categoryName:            CategoryName,
    retryDelay:              FiniteDuration,
    logger:                  Logger[Interpretation],
    retryInterval:           FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, EventStatusUpdater[Interpretation]](Throttler.noThrottling,
                                                                           logger,
                                                                           retryInterval = retryInterval,
                                                                           maxRetries = maxRetries,
                                                                           requestTimeoutOverride =
                                                                             requestTimeoutOverride
    )
    with EventStatusUpdater[Interpretation] {

  import cats.effect._
  import io.circe.literal._
  import org.http4s.Method.PATCH
  import org.http4s.Status.{NotFound, Ok}
  import org.http4s.{Request, Response}

  override def toTriplesGenerated(eventId:        CompoundEventId,
                                  payload:        JsonLDTriples,
                                  schemaVersion:  SchemaVersion,
                                  processingTime: EventProcessingTime
  ): Interpretation[Unit] = sendStatusChange(
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

  override def toTriplesStore(eventId: CompoundEventId, processingTime: EventProcessingTime): Interpretation[Unit] =
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

  override def rollback[S <: EventStatus](
      eventId:               CompoundEventId
  )(implicit rollbackStatus: () => S): Interpretation[Unit] =
    sendStatusChange(
      eventId,
      eventContent = EventRequestContent(json"""{"status": ${rollbackStatus().value}}"""),
      responseMapping
    )

  override def toFailure(eventId:     CompoundEventId,
                         eventStatus: FailureStatus,
                         exception:   Throwable
  ): Interpretation[Unit] =
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
      eventId:      CompoundEventId,
      eventContent: EventRequestContent,
      responseMapping: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        Unit
      ]]
  ): Interpretation[Unit] = for {
    uri <- validateUri(s"$eventLogUrl/events/${eventId.id}/${eventId.projectId}")
    request = createRequest(uri, eventContent)
    sendingResult <- send(request)(responseMapping) recoverWith retryOnServerError(
                       Eval.always(sendStatusChange(eventId, eventContent, responseMapping))
                     )
  } yield sendingResult

  private def retryOnServerError(
      retry: Eval[Interpretation[Unit]]
  ): PartialFunction[Throwable, Interpretation[Unit]] = {
    case UnexpectedResponseException(ServiceUnavailable | GatewayTimeout | BadGateway, message) =>
      waitAndRetry(retry, message)
    case exception @ (_: ConnectivityException | _: ClientException) =>
      waitAndRetry(retry, exception.getMessage)
  }

  private def waitAndRetry(retry: Eval[Interpretation[Unit]], errorMessage: String) = for {
    _      <- logger.error(s"$categoryName: sending status change failed - retrying in $retryDelay - $errorMessage")
    _      <- Timer[Interpretation] sleep retryDelay
    result <- retry.value
  } yield result

  private def createRequest(uri: Uri, eventRequestContent: EventRequestContent) =
    request(PATCH, uri).withMultipartBuilder
      .addPart("event", eventRequestContent.event)
      .maybeAddPart("payload", eventRequestContent.maybePayload)
      .build()

  private lazy val responseMapping
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Ok, _, _)       => ().pure[Interpretation]
    case (NotFound, _, _) => ().pure[Interpretation]
  }
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
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventStatusUpdaterImpl(eventLogUrl, categoryName, retryDelay = 30 seconds, logger)
}
