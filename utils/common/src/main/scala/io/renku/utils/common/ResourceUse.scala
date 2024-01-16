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

package io.renku.utils.common

import cats.effect._
import cats.syntax.all._
import fs2._
import fs2.concurrent.{Signal, SignallingRef}

object ResourceUse {

  def apply[F[_]: Spawn: Concurrent, A](resource: Resource[F, A]): UseSyntax[F, A] = new UseSyntax(resource)

  final class UseSyntax[F[_]: Spawn: Concurrent, A](resource: Resource[F, A]) {

    /** Evaluates `resource` endlessly or until the signal turns `true`. */
    def useUntil(signal: Signal[F, Boolean], returnValue: Ref[F, ExitCode]): F[ExitCode] = {
      val server         = Stream.resource(resource)
      val blockUntilTrue = signal.discrete.takeWhile(_ == false).drain
      val exit           = fs2.Stream.eval(returnValue.get)
      (server *> (blockUntilTrue ++ exit)).compile.lastOrError
    }

    def useForever(implicit ev: Async[F]): F[ExitCode] = for {
      termSignal <- SignallingRef.of[F, Boolean](false)
      exitValue  <- Ref.of(ExitCode.Success)
      rc         <- useUntil(termSignal, exitValue)
    } yield rc
  }
}
