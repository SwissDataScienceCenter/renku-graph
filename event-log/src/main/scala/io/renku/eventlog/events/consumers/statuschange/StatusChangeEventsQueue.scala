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

package io.renku.eventlog.events.consumers.statuschange

import StatusChangeEventsQueue._
import cats.data.Kleisli
import cats.effect.{Async, Ref, Temporal}
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.circe.{Decoder, Encoder}
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.data.Completion
import skunk.{Session, ~}

import java.time.OffsetDateTime
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait StatusChangeEventsQueue[F[_]] {

  def register[E <: StatusChangeEvent](
      handler:        E => F[Unit]
  )(implicit decoder: Decoder[E], eventType: EventType[E]): F[Unit]

  def offer[E <: StatusChangeEvent](
      event:          E
  )(implicit encoder: Encoder[E], eventType: EventType[E], show: Show[E]): Kleisli[F, Session[F], Unit]

  def run(): F[Unit]
}

object StatusChangeEventsQueue {

  final case class EventType[E](value: String)
  object EventType {
    implicit def show[E]: Show[EventType[E]] = Show.show(_.value)
  }

  def apply[F[_]: Async: Logger: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[StatusChangeEventsQueue[F]] = MonadThrow[F].catchNonFatal {
    new StatusChangeEventsQueueImpl[F](queriesExecTimes)
  }
}

private class StatusChangeEventsQueueImpl[F[_]: Async: Logger: SessionResource](queriesExecTimes: LabeledHistogram[F])
    extends DbClient[F](Some(queriesExecTimes))
    with StatusChangeEventsQueue[F] {

  import eu.timepit.refined.auto._
  import io.circe.parser.parse
  import io.circe.syntax._
  import skunk.codec.all._
  import skunk.implicits._

  private case class HandlerDef[E <: StatusChangeEvent](eventType: EventType[E],
                                                        decoder:   Decoder[E],
                                                        handler:   E => F[Unit]
  )

  private val handlers: Ref[F, List[HandlerDef[_ <: StatusChangeEvent]]] = Ref.unsafe(List.empty)

  override def register[E <: StatusChangeEvent](
      handler:        E => F[Unit]
  )(implicit decoder: Decoder[E], eventType: EventType[E]): F[Unit] =
    handlers.update(HandlerDef(eventType, decoder, handler) :: _)

  override def offer[E <: StatusChangeEvent](
      event:          E
  )(implicit encoder: Encoder[E], eventType: EventType[E], show: Show[E]): Kleisli[F, Session[F], Unit] =
    measureExecutionTime {
      SqlStatement[F](name = "status change event queue - offer")
        .command[OffsetDateTime ~ String ~ String](
          sql"""INSERT INTO status_change_events_queue (date, event_type, payload)
                VALUES ($timestamptz, $varchar, $text)
          """.command
        )
        .arguments(OffsetDateTime.now() ~ eventType.value ~ event.asJson(encoder).noSpaces)
        .build
    } flatMapF {
      case Completion.Insert(1) => ().pure[F]
      case Completion.Insert(0) => Logger[F].error(show"$categoryName $event wasn't enqueued")
      case other                => Logger[F].error(s"$categoryName offering ${event.show} failed with $other")
    }

  override def run(): F[Unit] = dequeueAll().foreverM

  private def dequeueAll(): F[Unit] = Temporal[F].andWait(
    (handlers.get >>= (_.map(h => dequeueType(h)).sequence.void)) recoverWith loggingStatement,
    time = 1 second
  )

  private def dequeueType[E <: StatusChangeEvent](handler: HandlerDef[E]): F[Unit] =
    dequeueEvent[E](handler) >>= {
      case None                => ().pure
      case Some(stringPayload) => process(stringPayload, handler) >> dequeueType(handler)
    }

  private def dequeueEvent[E <: StatusChangeEvent](handler: HandlerDef[E]) = SessionResource[F].useK {
    findEvent[E](handler.eventType) >>= {
      case None                     => Kleisli.pure(Option.empty[String])
      case Some((eventId, payload)) => deleteEvent(eventId) map (if (_) payload.some else None)
    }
  }

  private def process[E <: StatusChangeEvent](stringPayload: String, handlerDef: HandlerDef[E]) = {
    for {
      json  <- MonadThrow[F].fromEither(parse(stringPayload))
      event <- MonadThrow[F].fromEither(json.as(handlerDef.decoder))
      _     <- handlerDef.handler(event)
    } yield ()
  } recoverWith loggingError(stringPayload, handlerDef)

  private def findEvent[E](eventType: EventType[E]): Kleisli[F, Session[F], Option[(Int, String)]] =
    measureExecutionTime {
      SqlStatement[F](name = "status change event queue - find")
        .select[String, Int ~ String] {
          sql"""
          SELECT id, payload 
          FROM status_change_events_queue
          WHERE event_type = $varchar
          ORDER BY date ASC
          LIMIT 1""".query(int4 ~ text)
        }
        .arguments(eventType.value)
        .build(_.option)
    }

  private def deleteEvent(id: Int): Kleisli[F, Session[F], Boolean] = measureExecutionTime {
    SqlStatement[F](name = "status change event queue - delete")
      .command[Int](sql"""DELETE FROM status_change_events_queue WHERE id = $int4""".command)
      .arguments(id)
      .build
      .flatMapResult {
        case Completion.Delete(1) => true.pure[F]
        case _                    => false.pure[F]
      }
  }

  private def loggingError[E <: StatusChangeEvent](eventPayload: String,
                                                   handlerDef:   HandlerDef[E]
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(ex) =>
    Logger[F].error(ex)(
      show"$categoryName processing event $eventPayload of type ${handlerDef.eventType.value} failed"
    )
  }

  private lazy val loggingStatement: PartialFunction[Throwable, F[Unit]] = { case NonFatal(ex) =>
    Temporal[F].andWait(
      Logger[F].error(ex)(show"$categoryName processing events from the queue failed"),
      1 second
    )
  }
}
