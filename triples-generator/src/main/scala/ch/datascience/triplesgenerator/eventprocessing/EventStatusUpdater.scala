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

package ch.datascience.triplesgenerator.eventprocessing

import java.io.{PrintWriter, StringWriter}
import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.http.client.IORestClient
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status

import scala.concurrent.ExecutionContext

private trait EventStatusUpdater[Interpretation[_]] {
  def markEventNew(eventId:                  CompoundEventId):           Interpretation[Unit]
  def markEventDone(eventId:                 CompoundEventId): Interpretation[Unit]
  def markEventFailedRecoverably(eventId:    CompoundEventId, exception: Throwable): Interpretation[Unit]
  def markEventFailedNonRecoverably(eventId: CompoundEventId, exception: Throwable): Interpretation[Unit]
  def markEventSkipped(eventId:              CompoundEventId, message:   String):    Interpretation[Unit]
}

private class IOEventStatusUpdater(
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
  import io.circe.syntax._
  import org.http4s.Method.PATCH
  import org.http4s.Status.{Conflict, NotFound, Ok}
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  override def markEventNew(eventId: CompoundEventId): IO[Unit] = sendStatusChange(
    eventId,
    payload = json"""{"status": "NEW"}""",
    responseMapping = okConflictAsSuccess
  )

  override def markEventDone(eventId: CompoundEventId): IO[Unit] = sendStatusChange(
    eventId,
    payload = json"""{"status": "TRIPLES_STORE"}""",
    responseMapping = okConflictAsSuccess
  )

  override def markEventFailedRecoverably(eventId: CompoundEventId, exception: Throwable): IO[Unit] =
    sendStatusChange(
      eventId,
      payload = json"""{"status": "GENERATION_RECOVERABLE_FAILURE"}""" deepMerge exception.asJson,
      responseMapping = okConflictAsSuccess
    )

  override def markEventFailedNonRecoverably(eventId: CompoundEventId, exception: Throwable): IO[Unit] =
    sendStatusChange(
      eventId,
      payload = json"""{"status": "GENERATION_NON_RECOVERABLE_FAILURE"}""" deepMerge exception.asJson,
      responseMapping = okConflictAsSuccess
    )

  def markEventSkipped(eventId: CompoundEventId, message: String): IO[Unit] =
    sendStatusChange(
      eventId,
      payload = json"""{"status": "SKIPPED", "message": $message}""",
      responseMapping = okConflictAsSuccess
    )

  private def sendStatusChange(
      eventId:         CompoundEventId,
      payload:         Json,
      responseMapping: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]]
  ): IO[Unit] =
    for {
      uri           <- validateUri(s"$eventLogUrl/events/${eventId.id}/${eventId.projectId}")
      sendingResult <- send(request(PATCH, uri).withEntity(payload))(responseMapping)
    } yield sendingResult

  private lazy val okConflictAsSuccess: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Ok, _, _)       => IO.unit
    case (Conflict, _, _) => IO.unit
    case (NotFound, _, _) => IO.unit
  }

  private implicit val exceptionEncoder: Encoder[Throwable] = Encoder.instance[Throwable] { exception =>
    val exceptionAsString = new StringWriter
    exception.printStackTrace(new PrintWriter(exceptionAsString))
    exceptionAsString.flush()

    exceptionAsString.toString.trim match {
      case ""      => Json.obj()
      case message => json"""{"message": $message}"""
    }
  }
}

private object IOEventStatusUpdater {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventStatusUpdater[IO]] =
    for {
      eventLogUrl <- EventLogUrl[IO]()
    } yield new IOEventStatusUpdater(eventLogUrl, logger)
}
