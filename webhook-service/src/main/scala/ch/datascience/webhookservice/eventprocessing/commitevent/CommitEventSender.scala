/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing.commitevent

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.EventBody
import ch.datascience.http.client.IORestClient
import ch.datascience.webhookservice.eventprocessing.CommitEvent
import ch.datascience.webhookservice.eventprocessing.commitevent.CommitEventSender.EventSendingResult
import ch.datascience.webhookservice.eventprocessing.commitevent.CommitEventSender.EventSendingResult.{EventCreated, EventExisted}
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait CommitEventSender[Interpretation[_]] {
  def send(commitEvent: CommitEvent): Interpretation[EventSendingResult]
}

object CommitEventSender {
  sealed trait EventSendingResult extends Product with Serializable
  object EventSendingResult {
    case object EventCreated extends EventSendingResult
    case object EventExisted extends EventSendingResult
  }
}

class IOCommitEventSender(
    eventLogUrl:           EventLogUrl,
    commitEventSerializer: CommitEventSerializer[IO],
    logger:                Logger[IO]
)(implicit ME:             MonadError[IO, Throwable],
  executionContext:        ExecutionContext,
  contextShift:            ContextShift[IO],
  timer:                   Timer[IO])
    extends IORestClient(Throttler.noThrottling, logger)
    with CommitEventSender[IO] {

  import cats.effect._
  import commitEventSerializer._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import org.http4s.Method.POST
  import org.http4s.Status.{Created, Ok}
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  def send(commitEvent: CommitEvent): IO[EventSendingResult] =
    for {
      serialisedEvent <- serialiseToJsonString(commitEvent)
      eventBody       <- ME.fromEither(EventBody.from(serialisedEvent))
      uri             <- validateUri(s"$eventLogUrl/events")
      sendingResult   <- send(request(POST, uri).withEntity((commitEvent -> eventBody).asJson))(mapResponse)
    } yield sendingResult

  private implicit lazy val entityEncoder: Encoder[(CommitEvent, EventBody)] =
    Encoder.instance[(CommitEvent, EventBody)] {
      case (event, body) => json"""{
        "id":        ${event.id.value},
        "project": {
          "id":      ${event.project.id.value},
          "path":    ${event.project.path.value}
        },
        "date":      ${event.committedDate.value},
        "batchDate": ${event.batchDate.value},
        "body":      ${body.value}
      }"""
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[EventSendingResult]] = {
    case (Created, _, _) => EventCreated.pure[IO]
    case (Ok, _, _)      => EventExisted.pure[IO]
  }
}

object IOCommitEventSender {

  def apply(
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO],
    timer:                     Timer[IO]): IO[CommitEventSender[IO]] =
    for {
      eventLogUrl <- EventLogUrl[IO]()
    } yield new IOCommitEventSender(eventLogUrl, new CommitEventSerializer[IO], logger)
}
