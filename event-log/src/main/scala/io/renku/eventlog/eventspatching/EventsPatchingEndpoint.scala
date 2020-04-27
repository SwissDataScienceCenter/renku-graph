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

package io.renku.eventlog.eventspatching

import cats.effect.{ContextShift, IO}
import ch.datascience.controllers.ErrorMessage
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventStatus
import io.renku.eventlog.EventStatus.New
import org.http4s.circe.jsonOf
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}

import scala.language.higherKinds
import scala.util.control.NonFatal

trait EventsPatchingEndpoint[Interpretation[_]] {
  def triggerEventsPatching(request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class EventsPatchingEndpointImpl(
    eventsPatcher:        EventsPatcher[IO],
    waitingEventsGauge:   LabeledGauge[IO, projects.Path],
    underProcessingGauge: LabeledGauge[IO, projects.Path],
    logger:               Logger[IO]
)(implicit contextShift:  ContextShift[IO])
    extends Http4sDsl[IO]
    with EventsPatchingEndpoint[IO] {

  import cats.implicits._
  import ch.datascience.controllers.InfoMessage
  import ch.datascience.controllers.InfoMessage._
  import eventsPatcher._
  import org.http4s._

  override def triggerEventsPatching(request: Request[IO]): IO[Response[IO]] = {
    for {
      patch  <- request.as[EventsPatch[IO]] recoverWith badRequest
      _      <- applyToAllEvents(patch).start
      result <- Accepted(InfoMessage("Events patching triggered"))
    } yield result
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, IO[EventsPatch[IO]]] = {
    case NonFatal(exception) => IO.raiseError(BadRequestError(exception))
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private lazy val httpResponse: PartialFunction[Throwable, IO[Response[IO]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Events patching failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private implicit lazy val eventEntityDecoder: EntityDecoder[IO, EventsPatch[IO]] = {
    import io.circe.{Decoder, HCursor}

    implicit val eventDecoder: Decoder[EventsPatch[IO]] = (cursor: HCursor) =>
      for {
        status <- cursor.downField("status").as[EventStatus]
      } yield status match {
        case New   => StatusNewPatch[IO](waitingEventsGauge, underProcessingGauge)
        case other => throw new Exception(s"Patching events to '$other' status unsupported")
      }

    jsonOf[IO, EventsPatch[IO]]
  }
}

object IOEventsPatchingEndpoint {
  import ch.datascience.db.DbTransactor
  import io.renku.eventlog.EventLogDB

  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      logger:               Logger[IO]
  )(implicit contextShift:  ContextShift[IO]): IO[EventsPatchingEndpoint[IO]] = IO {
    new EventsPatchingEndpointImpl(
      new IOEventsPatcher(transactor, logger),
      waitingEventsGauge,
      underProcessingGauge,
      logger
    )
  }
}
