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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.CleanUpRequest
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.ProcessExecutor
import io.renku.events.{CategoryName, consumers}
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    processor:                 EventProcessor[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](ProcessExecutor.sequential) {

  protected override type Event = CleanUpRequest

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[CleanUpRequest],
      process = processor.process
    )
}

private object EventHandler {
  def apply[F[_]: Async: Logger: SessionResource: QueriesExecutionTimes]: F[consumers.EventHandler[F]] =
    EventProcessor[F].map(new EventHandler[F](_))
}
