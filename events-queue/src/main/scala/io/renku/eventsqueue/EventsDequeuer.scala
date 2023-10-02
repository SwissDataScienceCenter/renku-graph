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

package io.renku.eventsqueue

import cats.effect.Async
import cats.syntax.all._
import fs2.Pipe
import io.renku.db.SessionResource
import io.renku.db.syntax.CommandDef
import io.renku.events.CategoryName
import io.renku.lock.Lock
import org.typelevel.log4cats.Logger
import skunk.{Session, SqlState}

trait EventsDequeuer[F[_]] {
  def registerHandler(category: CategoryName, handler: Pipe[F, DequeuedEvent, DequeuedEvent]): F[Unit]
}

private class EventsDequeuerImpl[F[_]: Async: Logger, DB](repository: DBRepository[F], lock: Lock[F, CategoryName])(
    implicit sr: SessionResource[F, DB]
) extends EventsDequeuer[F] {

  override def registerHandler(category: CategoryName, handler: Pipe[F, DequeuedEvent, DequeuedEvent]): F[Unit] =
    sr.useK(dequeueEvents(category, handler)) >>
      Async[F].start(dequeueWithErrorHandling(category, handler)).void

  private def dequeueEvents(category: CategoryName, handler: Pipe[F, DequeuedEvent, DequeuedEvent]): CommandDef[F] =
    CommandDef[F] { session =>
      val transformation: Pipe[F, DequeuedEvent, Unit] =
        _.through(updateProcessing(session))
          .through(handler)
          .attempt
          .evalMap(logErrorAndReturnNone(category))
          .collect { case Some(e) => e }
          .through(removeEvents(session))

      lock.run(category).surround {
        repository
          .processEvents(category, transformation)
          .run(session)
      }
    }

  private def dequeueWithErrorHandling(category: CategoryName,
                                       handler:  Pipe[F, DequeuedEvent, DequeuedEvent]
  ): F[Unit] =
    sr.useK(dequeueOnEvent(category, handler))
      .handleErrorWith(logStatement(category))
      .flatMap(_ => dequeueWithErrorHandling(category, handler))

  private def updateProcessing(session: Session[F]): Pipe[F, DequeuedEvent, DequeuedEvent] =
    _.evalTap(e => repository.markUnderProcessing(e.id)(session).handleErrorWith(skipDeadlocks))

  private def removeEvents(session: Session[F]): Pipe[F, DequeuedEvent, Unit] =
    _.evalMap(e => repository.delete(e.id)(session).handleErrorWith(skipDeadlocks))

  private def dequeueOnEvent(category: CategoryName, handler: Pipe[F, DequeuedEvent, DequeuedEvent]): CommandDef[F] =
    CommandDef[F] { session =>
      session
        .channel(category.asChannelId)
        .listen(maxQueued = 1)
        .evalMap(_ => dequeueEvents(category, handler)(session))
        .compile
        .drain
    }

  private def logStatement(category: CategoryName): Throwable => F[Unit] =
    Logger[F].error(_)(show"An error in the events dequeueing process for $category")

  private def logErrorAndReturnNone(
      category: CategoryName
  ): Either[Throwable, DequeuedEvent] => F[Option[DequeuedEvent]] =
    _.fold(
      Logger[F].error(_)(show"An error in the events dequeueing process for $category").as(Option.empty[DequeuedEvent]),
      Option(_).pure[F]
    )

  private lazy val skipDeadlocks: Throwable => F[Unit] = { case SqlState.DeadlockDetected(_) => ().pure[F] }
}
