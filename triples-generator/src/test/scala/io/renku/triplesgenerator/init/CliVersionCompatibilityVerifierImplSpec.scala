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

package io.renku.triplesgenerator.init

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.config.RenkuPythonDevVersion
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CliVersionCompatibilityVerifierImplSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "run" should {

    "succeed if the cli version matches the cli version from the compatibility config" in {

      val cliVersion   = cliVersions.generateOne
      val compatConfig = compatibilityGen.generateOne.copy(configuredCliVersion = cliVersion, renkuDevVersion = None)

      val checker = new CliVersionCompatibilityVerifierImpl[IO](cliVersion, compatConfig, maybeRenkuDevVersion = None)

      checker.run.unsafeRunSync() shouldBe ()
    }

    "fail if the cli version does not match the cli version from the compatibility config" in {

      val cliVersion   = cliVersions.generateOne
      val compatConfig = compatibilityGen.suchThat(c => c.cliVersion != cliVersion).generateOne

      val checker = new CliVersionCompatibilityVerifierImpl[IO](cliVersion, compatConfig, maybeRenkuDevVersion = None)

      val failure = intercept[Exception](checker.run.unsafeRunSync())

      failure shouldBe a[IllegalStateException]
      failure.getMessage shouldBe show"Incompatible versions. cliVersion: $cliVersion, configured version: ${compatConfig.cliVersion}"
    }

    "succeed if there's CliDevVersion configured even if the versions does not match" in {

      val cliVersion      = cliVersions.generateOne
      val compatConfig    = compatibilityGen.generateOne.copy(configuredCliVersion = cliVersion, renkuDevVersion = None)
      val renkuDevVersion = RenkuPythonDevVersion(cliVersions.generateOne.value)

      assume(cliVersion.value != renkuDevVersion.version)

      val checker = new CliVersionCompatibilityVerifierImpl[IO](cliVersion, compatConfig, renkuDevVersion.some)

      checker.run.unsafeRunSync() shouldBe ()
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
}
