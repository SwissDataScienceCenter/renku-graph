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

package io.renku.events.consumers

import cats.effect.{Concurrent, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, Busy}
import org.typelevel.log4cats.Logger

private class ConcurrentProcessExecutor[F[_]: Concurrent: Logger](semaphore: Semaphore[F]) extends ProcessExecutor[F] {

  def tryExecuting(process: F[Unit]): F[EventSchedulingResult] = semaphore.tryAcquire >>= {
    case false =>
      Busy.pure[F].widen[EventSchedulingResult]
    case _ =>
      Concurrent[F]
        .start {
          Resource
            .make(().pure[F])(_ => semaphore.release)
            .evalTap(_ => process.handleErrorWith(Logger[F].error(_)("Scheduled process failed")))
            .use_
        }
        .as(Accepted)
  }
}
