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

package ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog

import ammonite.ops.{Bytes, CommandResult, Path, ShelloutException}
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuLogTimeout
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog.Commands.{Renku, RepositoryPath}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

class RenkuSpec extends AnyWordSpec with should.Matchers {

  "log" should {

    "return the 'renku log' output if it executes quicker than the defined timeout" in new TestCase {
      val commandBody = jsonLDTriples.generateOne
      val commandResult = CommandResult(
        exitCode = 0,
        chunks = Seq(Left(new Bytes(commandBody.value.noSpaces.getBytes())))
      )

      val Right(triples) = renku().log(event)(triplesGeneration(returning = commandResult), path).value.unsafeRunSync()

      triples shouldBe commandBody
    }

    "fail if calling 'renku log' results in a failure" in new TestCase {
      val exception = exceptions.generateOne

      intercept[Exception] {
        renku().log(event)(triplesGeneration(failingWith = exception), path).value.unsafeRunSync()
      } shouldBe exception
    }

    s"return $GenerationRecoverableError if calling 'renku log' results in a 137 exit code" in new TestCase {
      val exception = ShelloutException(CommandResult(exitCode = 137, chunks = Nil))

      val Left(error) = renku().log(event)(triplesGeneration(failingWith = exception), path).value.unsafeRunSync()

      error            shouldBe a[GenerationRecoverableError]
      error.getMessage shouldBe "Not enough memory"
    }

    "get terminated if calling 'renku log' takes longer than the defined timeout" in new TestCase {
      val timeout = RenkuLogTimeout(100 millis)

      intercept[Exception] {
        renku(timeout).log(event)(triplesGenerationTakingTooLong, path).value.unsafeRunSync()
      }.getMessage shouldBe s"'renku log' execution for commit: ${event.commitId}, project: ${event.project.id} " +
        s"took longer than $timeout - terminating"
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {
    val event = commitEvents.generateOne
    val path  = RepositoryPath(paths.generateOne)

    private val renkuLogTimeout = RenkuLogTimeout(1500 millis)
    def renku(timeout: RenkuLogTimeout = renkuLogTimeout) = new Renku(timeout)

    def triplesGeneration(returning: CommandResult): (CommitEvent, Path) => CommandResult =
      (_, _) => {
        Thread sleep (renkuLogTimeout.value - (1300 millis)).toMillis
        returning
      }

    def triplesGeneration(failingWith: Exception): (CommitEvent, Path) => CommandResult =
      (_, _) => throw failingWith

    val triplesGenerationTakingTooLong: (CommitEvent, Path) => CommandResult =
      (_, _) => {
        blocking(Thread sleep (renkuLogTimeout.value * 10).toMillis)
        CommandResult(exitCode = 0, chunks = Nil)
      }
  }
}
