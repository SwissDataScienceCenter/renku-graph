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

import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.Stream
import io.renku.db.SessionResource
import io.renku.events.CategoryName
import io.renku.lock.Lock
import org.typelevel.log4cats.Logger
import skunk.data.Notification
import skunk.{Session, SqlState}

trait EventsDequeuer[F[_]] {
  def acquireEventsStream(category: CategoryName):  Stream[F, DequeuedEvent]
  def returnToQueue(event:        DequeuedEvent): F[Unit]
}

object EventsDequeuer {
  def apply[F[_]: Async: Logger, DB](lock: Lock[F, CategoryName])(implicit
      sr: SessionResource[F, DB]
  ): EventsDequeuer[F] = new EventsDequeuerImpl[F, DB](EventsRepository[F, DB], lock)
}

private class EventsDequeuerImpl[F[_]: Async: Logger, DB](repository: EventsRepository[F], lock: Lock[F, CategoryName])(
    implicit sr: SessionResource[F, DB]
) extends EventsDequeuer[F] {

  override def acquireEventsStream(category: CategoryName): Stream[F, DequeuedEvent] =
    (fetchAllChunks(category) ++ fetchEventsOnNotif(category)).flatMap(Stream.emits)

  private def fetchAllChunks(category: CategoryName): Stream[F, List[DequeuedEvent]] =
    (Stream.resource(fetchEventsChunk(category)) ++ fetchAllChunks(category)).takeWhile(_.nonEmpty)

  private def fetchEventsChunk(category: CategoryName): Resource[F, List[DequeuedEvent]] =
    Resource
      .make(fetchChunkAndMarkProcessing(category))(chunk => sr.session.use(removeEvents(chunk)))

  private def fetchChunkAndMarkProcessing(category: CategoryName) =
    sr.session.use { session =>
      lock
        .run(category)
        .use(_ => repository.fetchEvents(category)(session).flatTap(updateProcessing(_)(session)))
    }

  private def fetchEventsOnNotif(category: CategoryName): Stream[F, List[DequeuedEvent]] =
    dequeueOnEvent(category) >> fetchAllChunks(category)

  private def dequeueOnEvent(category: CategoryName): Stream[F, Notification[String]] =
    Stream.resource {
      sr.session >>= { session =>
        session
          .channel(category.asChannelId)
          .listenR(maxQueued = 1)
      }
    }.flatten

  private def updateProcessing(events: List[DequeuedEvent])(session: Session[F]): F[Unit] =
    events.traverse_(e => repository.markUnderProcessing(e.id)(session).handleErrorWith(skipDeadlocks))

  private def removeEvents(events: List[DequeuedEvent])(session: Session[F]): F[Unit] =
    events.traverse_(e => repository.delete(e.id)(session).handleErrorWith(skipDeadlocks))

  private lazy val skipDeadlocks: Throwable => F[Unit] = { case SqlState.DeadlockDetected(_) => ().pure[F] }

  override def returnToQueue(event: DequeuedEvent): F[Unit] =
    sr.session.use(repository.returnToQueue(event.id).run)
}
