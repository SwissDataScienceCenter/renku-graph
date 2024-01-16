/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import fs2.{Chunk, Stream}
import io.circe.Encoder
import io.renku.db.SessionResource
import io.renku.db.syntax.CommandDef
import io.renku.events.CategoryName
import io.renku.lock.Lock
import org.typelevel.log4cats.Logger
import skunk.data.{Identifier, Notification}
import skunk.{Session, SqlState}

trait EventsQueue[F[_]] {

  def enqueue[E](category: CategoryName, event: E)(implicit enc: Encoder[E]): F[Unit] =
    enqueue(category, event, category.asChannelId)
  def enqueue[E](category: CategoryName, event: E, channel: Identifier)(implicit enc: Encoder[E]): F[Unit]

  def acquireEventsStream(category: CategoryName): Stream[F, Chunk[DequeuedEvent]] =
    acquireEventsStream(category, chunkSize = 50)
  def acquireEventsStream(category: CategoryName, chunkSize: Int): Stream[F, Chunk[DequeuedEvent]]

  def returnToQueue(event: DequeuedEvent): F[Unit]
}

object EventsQueue {
  def apply[F[_]: Async: Logger, DB](lock: Lock[F, CategoryName])(implicit
      sr: SessionResource[F, DB]
  ): EventsQueue[F] = new EventsQueueImpl[F, DB](EventsRepository[F, DB], lock)
}

private class EventsQueueImpl[F[_]: Async: Logger, DB](repository: EventsRepository[F], lock: Lock[F, CategoryName])(
    implicit sr: SessionResource[F, DB]
) extends EventsQueue[F] {

  override def enqueue[E](category: CategoryName, event: E, channel: Identifier)(implicit enc: Encoder[E]): F[Unit] =
    sr.useK {
      repository.insert(category, enc(event)) >> notify(channel, category)
    }

  private def notify(channel: Identifier, category: CategoryName): CommandDef[F] = CommandDef[F] {
    _.channel(channel).notify(category.value)
  }

  override def acquireEventsStream(category: CategoryName, chunkSize: Int): Stream[F, Chunk[DequeuedEvent]] =
    fetchAllChunks(category, chunkSize) ++ fetchEventsOnNotif(category, chunkSize)

  private def fetchAllChunks(category: CategoryName, chunkSize: Int): Stream[F, Chunk[DequeuedEvent]] =
    (Stream.resource(fetchEventsChunk(category, chunkSize)) ++ fetchAllChunks(category, chunkSize))
      .takeWhile(_.nonEmpty)

  private def fetchEventsChunk(category: CategoryName, chunkSize: Int): Resource[F, Chunk[DequeuedEvent]] =
    Resource
      .make(fetchChunkAndMarkProcessing(category, chunkSize))(chunk => sr.session.use(removeEvents(chunk)))

  private def fetchChunkAndMarkProcessing(category: CategoryName, chunkSize: Int): F[Chunk[DequeuedEvent]] =
    sr.session.use { session =>
      lock
        .run(category)
        .use(_ =>
          repository.fetchEvents(category, chunkSize)(session).map(Chunk.from).flatTap(updateProcessing(_)(session))
        )
    }

  private def fetchEventsOnNotif(category: CategoryName, chunkSize: Int): Stream[F, Chunk[DequeuedEvent]] =
    dequeueOnEvent(category) >> fetchAllChunks(category, chunkSize)

  private def dequeueOnEvent(category: CategoryName): Stream[F, Notification[String]] =
    Stream.resource {
      sr.session >>= { session =>
        session
          .channel(category.asChannelId)
          .listenR(maxQueued = 1)
      }
    }.flatten

  private def updateProcessing(events: Chunk[DequeuedEvent])(session: Session[F]): F[Unit] =
    events.traverse_(e => repository.markUnderProcessing(e.id)(session).handleErrorWith(skipDeadlocks))

  private def removeEvents(events: Chunk[DequeuedEvent])(session: Session[F]): F[Unit] =
    events.traverse_(e => repository.delete(e.id)(session).handleErrorWith(skipDeadlocks))

  private lazy val skipDeadlocks: Throwable => F[Unit] = { case SqlState.DeadlockDetected(_) => ().pure[F] }

  override def returnToQueue(event: DequeuedEvent): F[Unit] =
    sr.session.use(repository.returnToQueue(event.id).run)
}
