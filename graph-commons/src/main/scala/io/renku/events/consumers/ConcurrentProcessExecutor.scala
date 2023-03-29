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

import cats.effect.{Async, Concurrent, Deferred}
import cats.effect.std.Semaphore
import cats.syntax.all._
import io.renku.events.consumers.EventSchedulingResult.{Accepted, Busy}
import org.typelevel.log4cats.Logger

private class ConcurrentProcessExecutor[F[_]: Async: Logger](semaphore: Semaphore[F]) extends ProcessExecutor[F] {

  def tryExecuting(process: F[Unit]): F[EventSchedulingResult] = for {
    eventScheduled <- Deferred[F, EventSchedulingResult]
    _ <- Concurrent[F]
           .start {
             semaphore.tryPermit.use {
               case false => eventScheduled.complete(Busy.widen).void
               case true =>
                 eventScheduled.complete(Accepted.widen) >>
                   process.handleErrorWith(Logger[F].error(_)("Scheduled process failed"))
             }
           }
    result <- eventScheduled.get
  } yield result
}
