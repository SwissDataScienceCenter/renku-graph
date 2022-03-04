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

package io.renku.eventlog.events.categories.creation

import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, MonadCancelThrow}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.circe.{ACursor, Decoder, DecodingFailure}
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.events.categories.creation.Event.{NewEvent, SkippedEvent}
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers._
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.model.events.{BatchDate, EventBody, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadThrow: Concurrent: Logger](
    override val categoryName: CategoryName,
    eventPersister:            EventPersister[F]
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import eventPersister._
  import io.renku.graph.model.projects
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](storeEvent(request))

  private def storeEvent(request: EventRequestContent) = for {
    event <- fromEither[F](request.event.as[Event].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult])
    result <- storeNewEvent(event).toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(event))
                .leftSemiflatTap(Logger[F].log(event))
  } yield result

  private implicit lazy val eventInfoToString: Show[Event] = Show.show { event =>
    s"${event.compoundEventId}, projectPath = ${event.project.path}, status = ${event.status}"
  }

  private implicit val eventDecoder: Decoder[Event] = cursor =>
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
          message   <- extractMessage(cursor)
        } yield SkippedEvent(id, project, date, batchDate, body, message)
      case Some(invalidStatus) =>
        Left(DecodingFailure(s"Status $invalidStatus is not valid. Only NEW or SKIPPED are accepted", Nil))
    }

  private lazy val extractMessage: ACursor => Decoder.Result[EventMessage] =
    _.downField("message")
      .as(blankStringToNoneDecoder(EventMessage))
      .flatMap {
        case None          => DecodingFailure(s"Skipped Status requires message", Nil).asLeft
        case Some(message) => message.asRight
      }
      .leftMap(_ => DecodingFailure("Invalid Skipped Event message", Nil))

  implicit val projectDecoder: Decoder[Project] = cursor =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)
}

private object EventHandler {
  def apply[F[_]: MonadCancelThrow: Concurrent: Logger](sessionResource: SessionResource[F, EventLogDB],
                                                        waitingEventsGauge: LabeledGauge[F, projects.Path],
                                                        queriesExecTimes:   LabeledHistogram[F, SqlStatement.Name]
  ): F[EventHandler[F]] = for {
    eventPersister <- EventPersister(sessionResource, waitingEventsGauge, queriesExecTimes)
  } yield new EventHandler[F](categoryName, eventPersister)
}
