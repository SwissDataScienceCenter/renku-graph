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

package io.renku.eventlog.events.categories.migrationstatuschange

import cats.data.EitherT.fromEither
import cats.effect.Async
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.ServiceVersion
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.MigrationStatus._
import io.renku.eventlog.events.categories.migrationstatuschange.Event.{ToDone, ToNonRecoverableFailure, ToRecoverableFailure}
import io.renku.eventlog.{MigrationMessage, MigrationStatus}
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: Async: Logger](
    statusUpdater:             StatusUpdater[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  import statusUpdater._

  protected override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](processEvent(request))

  private def processEvent(request: EventRequestContent) = for {
    event <- fromEither[F](request.event.as[Event].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult])
    result <- updateStatus(event).toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(event))
                .leftSemiflatTap(Logger[F].log(event))
  } yield result

  private implicit val eventDecoder: Decoder[Event] = { cursor =>
    for {
      url     <- cursor.downField("subscriber").downField("url").as[SubscriberUrl]
      version <- cursor.downField("subscriber").downField("version").as[ServiceVersion]
      event <- cursor.downField("newStatus").as[MigrationStatus] >>= {
                 case Done => ToDone(url, version).asRight
                 case NonRecoverableFailure =>
                   cursor.downField("message").as[MigrationMessage].map(ToNonRecoverableFailure(url, version, _))
                 case RecoverableFailure =>
                   cursor.downField("message").as[MigrationMessage].map(ToRecoverableFailure(url, version, _))
                 case other => DecodingFailure(show"Cannot change migration status to $other", Nil).asLeft
               }
    } yield event
  }
}

private object EventHandler {
  def apply[F[_]: Async: SessionResource: Logger](queriesExecTimes: LabeledHistogram[F]): F[EventHandler[F]] = for {
    statusUpdater <- StatusUpdater[F](queriesExecTimes)
  } yield new EventHandler[F](statusUpdater)
}
