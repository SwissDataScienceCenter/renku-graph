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

package io.renku.eventlog.events.categories.creation

import cats.MonadThrow
import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers._
import ch.datascience.graph.model.events.{BatchDate, CategoryName, EventBody, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.renku.eventlog.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class EventHandler[Interpretation[_]: MonadThrow: Concurrent](
    override val categoryName: CategoryName,
    eventPersister:            EventPersister[Interpretation],
    logger:                    Logger[Interpretation]
) extends consumers.EventHandlerWithProcessLimiter[Interpretation](ConcurrentProcessesLimiter.withoutLimit) {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import eventPersister._

  override def createHandlingProcess(
      request: EventRequestContent
  ): Interpretation[EventHandlingProcess[Interpretation]] =
    EventHandlingProcess[Interpretation](storeEvent(request))

  private def storeEvent(request: EventRequestContent) = for {
    event <-
      fromEither[Interpretation](request.event.as[Event].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult])
    result <- storeNewEvent(event).toRightT
                .map(_ => Accepted)
                .semiflatTap(logger.log(event))
                .leftSemiflatTap(logger.log(event))
  } yield result

  private implicit lazy val eventInfoToString: Event => String = { event =>
    s"${event.compoundEventId}, projectPath = ${event.project.path}, status = ${event.status}"
  }

  private implicit val eventDecoder: Decoder[Event] = (cursor: HCursor) =>
    cursor.downField("status").as[Option[EventStatus]] flatMap {
      case None | Some(EventStatus.New) =>
        for {
          id        <- cursor.downField("id").as[EventId]
          project   <- cursor.downField("project").as[Project]
          date      <- cursor.downField("date").as[EventDate]
          batchDate <- cursor.downField("batchDate").as[BatchDate]
          body      <- cursor.downField("body").as[EventBody]
        } yield NewEvent(id, project, date, batchDate, body)
      case Some(EventStatus.Skipped) =>
        for {
          id        <- cursor.downField("id").as[EventId]
          project   <- cursor.downField("project").as[Project]
          date      <- cursor.downField("date").as[EventDate]
          batchDate <- cursor.downField("batchDate").as[BatchDate]
          body      <- cursor.downField("body").as[EventBody]
          message   <- verifyMessage(cursor.downField("message").as[Option[String]])
        } yield SkippedEvent(id, project, date, batchDate, body, message)
      case Some(invalidStatus) =>
        Left(DecodingFailure(s"Status $invalidStatus is not valid. Only NEW or SKIPPED are accepted", Nil))
    }

  private def verifyMessage(result: Decoder.Result[Option[String]]) =
    result
      .map(blankToNone)
      .flatMap {
        case None          => Left(DecodingFailure(s"Skipped Status requires message", Nil))
        case Some(message) => EventMessage.from(message.value)
      }
      .leftMap(_ => DecodingFailure("Invalid Skipped Event message", Nil))

  implicit val projectDecoder: Decoder[Project] = (cursor: HCursor) =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)
}

private object EventHandler {
  def apply(sessionResource:    SessionResource[IO, EventLogDB],
            waitingEventsGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:   LabeledHistogram[IO, SqlStatement.Name],
            logger:             Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    eventPersister <- IOEventPersister(sessionResource, waitingEventsGauge, queriesExecTimes)
  } yield new EventHandler[IO](categoryName, eventPersister, logger)
}
