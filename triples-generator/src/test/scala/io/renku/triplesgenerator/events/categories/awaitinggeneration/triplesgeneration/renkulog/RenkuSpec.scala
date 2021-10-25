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

package io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog

import ammonite.ops.{Bytes, CommandResult, Path, ShelloutException}
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.jsonld.generators.JsonLDGenerators.jsonLDEntities
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.Commands.{Renku, RepositoryPath}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RenkuSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "export" should {

    "return the 'renku export' output if renku export succeeds" in new TestCase {
      val commandBody = jsonLDEntities.generateOne
      val commandResult = CommandResult(
        exitCode = 0,
        chunks = Seq(Left(new Bytes(commandBody.toJson.noSpaces.getBytes())))
      )

      renkuExport.expects(path.value).returning(commandResult)

      val Right(triples) = renku.export(path).value.unsafeRunSync()

      triples shouldBe commandBody
    }

    "fail if calling 'renku export' results in a failure" in new TestCase {
      val exception = exceptions.generateOne

      renkuExport.expects(path.value).throws(exception)

      intercept[Exception] {
        renku.export(path).value.unsafeRunSync()
      } shouldBe exception
    }

    s"return $GenerationRecoverableError if calling 'renku export' results in a 137 exit code" in new TestCase {
      val exception = ShelloutException(CommandResult(exitCode = 137, chunks = Nil))
      renkuExport.expects(path.value).throws(exception)

      val Left(error) = renku.export(path).value.unsafeRunSync()

      error            shouldBe a[GenerationRecoverableError]
      error.getMessage shouldBe "Not enough memory"
    }
  }

  private trait TestCase {
    val path        = RepositoryPath(paths.generateOne)
    val renkuExport = mockFunction[Path, CommandResult]

    val renku = new Renku[IO](renkuExport)
  }
}
