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

package io.renku.eventlog.creation

import EventPersister.Result
import cats.MonadError
import cats.effect.Effect
import cats.implicits._
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.events.{BatchDate, EventBody, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog._
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, Request, Response}

import scala.language.higherKinds
import scala.util.control.NonFatal

class EventCreationEndpoint[Interpretation[_]: Effect](
    eventAdder: EventPersister[Interpretation],
    logger:     Logger[Interpretation]
)(implicit ME:  MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import EventCreationEndpoint._
  import eventAdder._
  import org.http4s.circe._

  def addEvent(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      event         <- request.as[Event] recoverWith badRequest
      storingResult <- storeNewEvent(event)
      response      <- storingResult.asHttpResponse
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[Event]] = {
    case NonFatal(exception) =>
      ME.raiseError(BadRequestError(exception))
  }

  private implicit class ResultOps(result: Result) {
    lazy val asHttpResponse: Interpretation[Response[Interpretation]] = result match {
      case Result.Created => Created(InfoMessage("Event created"))
      case Result.Existed => Ok(InfoMessage("Event existed"))
    }
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Event creation failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private implicit lazy val eventEntityDecoder: EntityDecoder[Interpretation, Event] =
    jsonOf[Interpretation, Event]
}

object EventCreationEndpoint {
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.{Decoder, HCursor}

  private implicit val eventDecoder: Decoder[Event] = (cursor: HCursor) =>
    for {
      id        <- cursor.downField("id").as[EventId]
      project   <- cursor.downField("project").as[EventProject]
      date      <- cursor.downField("date").as[EventDate]
      batchDate <- cursor.downField("batchDate").as[BatchDate]
      body      <- cursor.downField("body").as[EventBody]
    } yield Event(id, project, date, batchDate, body)

  implicit val projectDecoder: Decoder[EventProject] = (cursor: HCursor) =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield EventProject(id, path)
}

object IOEventCreationEndpoint {
  import cats.effect.{ContextShift, IO}

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      waitingEventsGauge:  LabeledGauge[IO, projects.Path],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[EventCreationEndpoint[IO]] = IO {
    new EventCreationEndpoint[IO](new IOEventPersister(transactor, waitingEventsGauge), logger)
  }
}
