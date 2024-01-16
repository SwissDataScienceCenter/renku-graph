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

package io.renku.eventlog.api.events

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.events.producers.EventSender
import io.renku.graph.config.EventLogUrl
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

trait Client[F[_]] {
  def send[E](event: E)(implicit dispatcher: Dispatcher[F, E]): F[Unit]
}

object Client {

  def apply[F[_]: Async: Logger: MetricsRegistry](config: Config): F[Client[F]] =
    EventSender[F](EventLogUrl, config).map(new ClientImpl[F](_))

  def apply[F[_]: Async: Logger: MetricsRegistry]: F[Client[F]] =
    EventSender[F](EventLogUrl).map(new ClientImpl[F](_))
}

private class ClientImpl[F[_]](eventSender: EventSender[F]) extends Client[F] {

  override def send[E](event: E)(implicit dispatcher: Dispatcher[F, E]): F[Unit] =
    dispatcher.dispatch(event, eventSender)
}
