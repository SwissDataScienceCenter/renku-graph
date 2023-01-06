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

package io.renku.triplesgenerator.init

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CliVersionCompatibilityVerifierImplSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {
    "return Unit if the cli version matches the cli version from the compatibility config" in {
      val cliVersion   = cliVersions.generateOne
      val compatConfig = compatibilityGen.generateOne.copy(configuredCliVersion = cliVersion, renkuDevVersion = None)
      val checker      = new CliVersionCompatibilityVerifierImpl[Try](cliVersion, compatConfig)

      checker.run() shouldBe Success(())
    }

    "fail if the cli version does not match the cli version from the compatibility config" in {
      val cliVersion         = cliVersions.generateOne
      val compatConfig       = compatibilityGen.suchThat(c => c.cliVersion != cliVersion).generateOne
      val checker            = new CliVersionCompatibilityVerifierImpl[Try](cliVersion, compatConfig)
      val Failure(exception) = checker.run()
      exception shouldBe a[IllegalStateException]
      exception.getMessage shouldBe s"Incompatible versions. cliVersion: $cliVersion, configured version: ${compatConfig.cliVersion}"
    }
  }
}
