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
import io.circe.Encoder
import io.renku.db.SessionResource
import io.renku.db.syntax.CommandDef
import io.renku.events.CategoryName
import skunk.data.Identifier

trait EventsEnqueuer[F[_]] {
  def enqueue[E](category: CategoryName, event: E)(implicit enc: Encoder[E]): F[Unit]
  def enqueue[E](category: CategoryName, event: E, channel:      Identifier)(implicit enc: Encoder[E]): F[Unit]
}

object EventsEnqueuer {
  def apply[F[_]: Async, DB](implicit sr: SessionResource[F, DB]): EventsEnqueuer[F] =
    new EventsEnqueuerImpl[F, DB](EventsRepository[F, DB])
}

private class EventsEnqueuerImpl[F[_]: Async, DB](repository: EventsRepository[F])(implicit sr: SessionResource[F, DB])
    extends EventsEnqueuer[F] {

  override def enqueue[E](category: CategoryName, event: E)(implicit enc: Encoder[E]): F[Unit] =
    enqueue(category, event, category.asChannelId)

  override def enqueue[E](category: CategoryName, event: E, channel: Identifier)(implicit enc: Encoder[E]): F[Unit] = {
    val encodedEvent = enc(event)
    sr.useK {
      repository.insert(category, encodedEvent) >> notify(channel, category)
    }
  }

  private def notify(channel: Identifier, category: CategoryName): CommandDef[F] = CommandDef[F] {
    _.channel(channel).notify(category.value)
  }
}
