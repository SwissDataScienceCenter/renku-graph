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

package io.renku.events.consumers

import cats.syntax.all._
import cats.MonadThrow
import cats.effect.Async
import cats.effect.std.Semaphore
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.typelevel.log4cats.Logger

trait ProcessExecutor[F[_]] {
  def tryExecuting(process: F[Unit]): F[EventSchedulingResult]
}

object ProcessExecutor {
  def apply[F[_]](f: F[Unit] => F[EventSchedulingResult]): ProcessExecutor[F] =
    (process: F[Unit]) => f(process)

  def sequential[F[_]: MonadThrow]: ProcessExecutor[F] =
    ProcessExecutor { process =>
      process
        .as(EventSchedulingResult.Accepted.widen)
        .handleError(error => EventSchedulingResult.SchedulingError(error))
    }

  def concurrent[F[_]: Async: Logger](processesCount: Int Refined Positive): F[ProcessExecutor[F]] =
    Semaphore(processesCount.value).map(new ConcurrentProcessExecutor[F](_))
}
