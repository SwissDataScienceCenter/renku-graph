/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.creation

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.circe.{ACursor, Decoder, DecodingFailure}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.consumers.creation.{Event => CategoryEvent}
import io.renku.eventlog.events.consumers.creation.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.{consumers, CategoryName}
import io.renku.events.consumers._
import io.renku.events.consumers.EventDecodingTools._
import io.renku.graph.model.events.{BatchDate, EventBody, EventDate, EventId, EventMessage, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.{ProjectViewedEvent, UserId}
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    eventPersister:            EventPersister[F],
    tgClient:                  triplesgenerator.api.events.Client[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](ProcessExecutor.sequential) {

  import eventPersister.storeNewEvent

  protected override type Event = CategoryEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[Event],
      processEvent
    )

  private def processEvent(event: Event): F[Unit] =
    Logger[F].info(show"$categoryName: $event accepted") >>
      storeNewEvent(event) >>=
      sendProjectViewed()

  private def sendProjectViewed(): EventPersister.Result => F[Unit] = {
    case EventPersister.Result.Created(event) =>
      val maybeUserId = (event.body.maybeAuthorEmail orElse event.body.maybeCommitterEmail).map(UserId(_))
      tgClient
        .send(ProjectViewedEvent(event.project.path, projects.DateViewed(event.date.value), maybeUserId))
        .handleErrorWith(
          Logger[F].error(_)(show"$categoryName: sending ${ProjectViewedEvent.categoryName} event failed")
        )
    case _ => ().pure[F]
  }

  private implicit val eventDecoder: Decoder[Event] = cursor =>
    cursor.downField("status").as[Option[EventStatus]] flatMap {
      case None | Some(EventStatus.New) =>
        for {
          id        <- cursor.downField("id").as[EventId]
          project   <- cursor.value.getProject
          date      <- cursor.downField("date").as[EventDate]
          batchDate <- cursor.downField("batchDate").as[BatchDate]
          body      <- cursor.downField("body").as[EventBody]
        } yield NewEvent(id, project, date, batchDate, body)
      case Some(EventStatus.Skipped) =>
        for {
          id        <- cursor.downField("id").as[EventId]
          project   <- cursor.value.getProject
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
}

private object EventHandler {
  def apply[F[_]: Async: Logger: SessionResource: QueriesExecutionTimes: EventStatusGauges: MetricsRegistry]
      : F[consumers.EventHandler[F]] =
    (EventPersister[F], triplesgenerator.api.events.Client[F])
      .mapN(new EventHandler[F](_, _))
}
