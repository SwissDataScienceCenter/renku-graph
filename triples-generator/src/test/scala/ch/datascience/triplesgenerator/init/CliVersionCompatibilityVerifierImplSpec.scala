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

package ch.datascience.triplesgenerator.init

import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CliVersionCompatibilityVerifierImplSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {
    "return Unit if the cli version matches the cli version from the first pair in compatibility matrix" in {
      val cliVersion = cliVersions.generateOne
      val versionPairs = renkuVersionPairs.generateNonEmptyList() match {
        case NonEmptyList(head, tail) => NonEmptyList(head.copy(cliVersion = cliVersion), tail)
      }
      val checker = new CliVersionCompatibilityVerifierImpl[Try](cliVersion, versionPairs)

      checker.run() shouldBe Success(())
    }

    "fail if the cli version does not match the cli version from the first pair in compatibility matrix" in {
      val cliVersion         = cliVersions.generateOne
      val versionPairs       = renkuVersionPairs.generateNonEmptyList()
      val checker            = new CliVersionCompatibilityVerifierImpl[Try](cliVersion, versionPairs)
      val Failure(exception) = checker.run()
      exception            shouldBe a[IllegalStateException]
      exception.getMessage shouldBe s"Incompatible versions. cliVersion: $cliVersion versionPairs: ${versionPairs.head.cliVersion}"
    }

  }

}
