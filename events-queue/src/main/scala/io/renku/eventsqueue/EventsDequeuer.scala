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
import fs2.Stream
import io.renku.db.SessionResource
import io.renku.db.syntax._
import io.renku.events.CategoryName
import org.typelevel.log4cats.Logger

trait EventsDequeuer[F[_]] {
  def registerHandler(category: CategoryName, handler: Stream[F, String] => F[Unit]): F[Unit]
}

private class EventsDequeuerImpl[F[_]: Async: Logger, DB](repository: DBRepository[F])(implicit
    sr: SessionResource[F, DB]
) extends EventsDequeuer[F] {

  override def registerHandler(category: CategoryName, handler: Stream[F, String] => F[Unit]): F[Unit] =
    sr.useK(dequeueEvents(category, handler)) >>
      Async[F].start(deq(category, handler)).void

  private def dequeueEvents(category: CategoryName, handler: Stream[F, String] => F[Unit]) =
    repository.fetchChunk(category).flatMapF(handler(_))

  private def deq(category: CategoryName, handler: Stream[F, String] => F[Unit]): F[Unit] =
    sr.useK(dequeueOnEvent(category, handler))
      .handleErrorWith(logStatement(category))
      .flatMap(_ => deq(category, handler))

  private def dequeueOnEvent(category: CategoryName, handler: Stream[F, String] => F[Unit]): CommandDef[F] =
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
}
