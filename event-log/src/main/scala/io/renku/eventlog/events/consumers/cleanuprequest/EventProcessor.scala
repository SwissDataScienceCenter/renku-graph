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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.metrics.LabeledHistogram

private trait EventProcessor[F[_]] {
  def process(event: CleanUpRequestEvent): F[Unit]
}

private object EventProcessor {
  def apply[F[_]: Async: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[EventProcessor[F]] = for {
    projectIdFinder <- ProjectIdFinder[F](queriesExecTimes)
    queue           <- CleanUpEventsQueue[F](queriesExecTimes)
  } yield new EventProcessorImpl[F](projectIdFinder, queue)
}

private class EventProcessorImpl[F[_]: MonadThrow](projectIdFinder: ProjectIdFinder[F], queue: CleanUpEventsQueue[F])
    extends EventProcessor[F] {

  import projectIdFinder._

  override def process(event: CleanUpRequestEvent): F[Unit] = event match {
    case CleanUpRequestEvent.Full(id, path) => queue.offer(id, path)
    case CleanUpRequestEvent.Partial(path) =>
      findProjectId(path) >>= {
        case Some(id) => queue.offer(id, path)
        case None     => new Exception(show"Cannot find projectId for $path").raiseError
      }
  }
}
