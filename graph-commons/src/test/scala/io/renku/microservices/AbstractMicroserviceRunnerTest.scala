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

package io.renku.microservices

import cats.effect.{ExitCode, IO}
import fs2.concurrent.{Signal, SignallingRef}
import scala.language.reflectiveCalls
import scala.concurrent.duration._

trait AbstractMicroserviceRunnerTest {

  type Microservice = { def run(signal: Signal[IO, Boolean]): IO[ExitCode] }

  def all: List[CallCounter]

  def runner: Microservice

  def startFor(duration: FiniteDuration) =
    for {
      term <- SignallingRef.of[IO, Boolean](true)
      _ <-  (IO.sleep(duration) *> term.set(true)).start
      exit <- runner.run(term)
    } yield exit

  def startAndStopRunner: IO[ExitCode] =
    for {
      term <- SignallingRef.of[IO, Boolean](true)
      exit <- runner.run(term)
    } yield exit

  def startRunnerForever: IO[ExitCode] =
    for {
      term <- SignallingRef.of[IO, Boolean](false)
      exit <- runner.run(term)
    } yield exit

  def assertCalledAll = CallCounter.assertCalled(all)

  def assertCalledAllBut(exclude: CallCounter, excludes: CallCounter*) =
    CallCounter.assertCalled(all.diff(exclude :: excludes.toList)) >>
      CallCounter.assertNotCalled(excludes.toList)
}
