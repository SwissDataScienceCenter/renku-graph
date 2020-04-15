/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import ammonite.ops.{Bytes, CommandResult, Path}
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.dbeventlog.config.RenkuLogTimeout
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog.Commands.Renku
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

class RenkuSpec extends WordSpec {

  "log" should {

    "return the 'renku log' output if it executes quicker than the defined timeout" in new TestCase {
      val commandBody = jsonLDTriples.generateOne
      val commandResult = CommandResult(
        exitCode = 0,
        chunks   = Seq(Left(new Bytes(commandBody.value.noSpaces.getBytes())))
      )

      val triples = renku().log(event, path)(triplesGeneration(returning = commandResult)).unsafeRunSync()

      triples shouldBe commandBody
    }

    "fail if calling 'renku log' results in a failure" in new TestCase {
      val exception = exceptions.generateOne

      intercept[Exception] {
        renku().log(event, path)(triplesGeneration(failingWith = exception)).unsafeRunSync()
      } shouldBe exception
    }

    "get terminated if calling 'renku log' takes longer than the defined timeout" in new TestCase {
      val timeout = RenkuLogTimeout(100 millis)

      intercept[Exception] {
        renku(timeout).log(event, path)(triplesGenerationTakingTooLong).unsafeRunSync()
      }.getMessage shouldBe s"'renku log' execution for commit: ${event.commitId}, project: ${event.project.id} " +
        s"took longer than $timeout - terminating"
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer:        Timer[IO]        = IO.timer(ExecutionContext.global)

  private trait TestCase {
    val event = commitEvents.generateOne
    val path  = paths.generateOne

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
