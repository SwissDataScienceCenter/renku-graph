/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.testtools

import cats.effect.IO
import org.scalamock.handlers.CallHandler0
import org.scalamock.scalatest.MockFactory
import org.scalatest.Suite

import scala.language.reflectiveCalls

trait MockedRunnableCollaborators {
  self: Suite with MockFactory =>

  private type Runnable[R] = { def run(): IO[R] }

  def given[R](runnable: Runnable[R]) = new RunnableOps[R](runnable)

  class RunnableOps[R](runnable: Runnable[R]) {
    import cats.syntax.all._

    def succeeds(returning: R): CallHandler0[IO[R]] =
      (runnable.run _)
        .expects()
        .returning(returning.pure[IO])

    def fails(becauseOf: Exception): CallHandler0[IO[R]] =
      (runnable.run _)
        .expects()
        .returning(becauseOf.raiseError[IO, R])
  }
}
