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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.data.EitherT
import cats.data.EitherT.fromEither
import cats.effect.{Async, Concurrent}
import cats.syntax.all._
import io.circe.{Decoder, Json}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import io.renku.events.consumers.{ConcurrentProcessesLimiter, EventHandlingProcess, EventSchedulingResult}
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: Concurrent: Logger](
    processor:                 EventProcessor[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](ConcurrentProcessesLimiter.withoutLimit) {

  protected override def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]] =
    EventHandlingProcess[F](processEvent(request))

  private def processEvent(request: EventRequestContent): EitherT[F, EventSchedulingResult, Accepted] = for {
    event <- fromEither[F](request.event.as(event).leftMap(_ => BadRequest).leftWiden[EventSchedulingResult])
    result <- processor
                .process(event)
                .toRightT
                .map(_ => Accepted)
                .semiflatTap(Logger[F].log(event)(_))
                .leftSemiflatTap(Logger[F].log(event)(_))
  } yield result

  private val event: Decoder[CleanUpRequestEvent] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    _.downField("project").as[Json].map(_.hcursor) >>= { cursor =>
      (cursor.downField("id").as[Option[projects.Id]], cursor.downField("path").as[projects.Path]).mapN {
        case (Some(id), path) => CleanUpRequestEvent(id, path)
        case (None, path)     => CleanUpRequestEvent(path)
      }
    }
  }
}

private object EventHandler {
  def apply[F[_]: Async: Logger: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[EventHandler[F]] =
    EventProcessor[F](queriesExecTimes).map(new EventHandler[F](_))
}
