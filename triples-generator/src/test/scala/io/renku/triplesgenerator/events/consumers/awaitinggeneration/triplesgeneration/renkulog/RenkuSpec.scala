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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration.renkulog

import ammonite.ops.{Bytes, CommandResult, Path, ShelloutException}
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.ProcessingNonRecoverableError
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.EventProcessingGenerators.commitEvents
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration.renkulog.Commands.{RenkuImpl, RepositoryPath}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RenkuSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "migrate" should {

    "succeed if 'renku migrate' succeeds" in new TestCase {
      val commandResult = CommandResult(exitCode = 0, chunks = Seq.empty)
      renkuMigrate.expects(path.value).returning(commandResult)

      val event = commitEvents.generateOne

      renku.migrate(event)(path).unsafeRunSync() shouldBe ()
    }

    "fail with some ProcessingNonRecoverableError.MalformedRepository " +
      "if 'renku migrate' fails" in new TestCase {
        val exception = exceptions.generateOne
        renkuMigrate.expects(path.value).throws(exception)

        val event = commitEvents.generateOne

        val failure = intercept[ProcessingNonRecoverableError.MalformedRepository] {
          renku.migrate(event)(path).unsafeRunSync()
        }

        failure.getMessage shouldBe s"'renku migrate' failed for commit: ${event.commitId}, project: ${event.project.id}"
        failure.getCause shouldBe exception
      }
  }

  "graphExport" should {

    "return the 'renku export' output if renku export succeeds" in new TestCase {
      val commandBody = jsonLDEntities.generateOne
      val commandResult = CommandResult(
        exitCode = 0,
        chunks = Seq(Left(new Bytes(commandBody.toJson.noSpaces.getBytes())))
      )

      renkuExport.expects(path.value).returning(commandResult)

      val Right(triples) = renku.graphExport(path).value.unsafeRunSync()

      triples shouldBe commandBody
    }

    "fail if calling 'renku export' results with a ProcessingNonRecoverableError.MalformedRepository" in new TestCase {
      val exception = exceptions.generateOne
      renkuExport.expects(path.value).throws(exception)

      val failure = intercept[ProcessingNonRecoverableError.MalformedRepository] {
        renku.graphExport(path).value.unsafeRunSync()
      }

      failure.getMessage shouldBe "'renku graph export' failed"
      failure.getCause   shouldBe exception
    }

    "return LogWorthyRecoverableError if calling 'renku export' results in a 137 exit code" in new TestCase {
      val exception = ShelloutException(CommandResult(exitCode = 137, chunks = Nil))
      renkuExport.expects(path.value).throws(exception)

      val Left(error) = renku.graphExport(path).value.unsafeRunSync()

      error            shouldBe a[LogWorthyRecoverableError]
      error.getMessage shouldBe "Not enough memory"
    }
  }

  private trait TestCase {
    val path         = RepositoryPath(paths.generateOne)
    val renkuMigrate = mockFunction[Path, CommandResult]
    val renkuExport  = mockFunction[Path, CommandResult]

    val renku = new RenkuImpl[IO](renkuMigrate, renkuExport)
  }
}
