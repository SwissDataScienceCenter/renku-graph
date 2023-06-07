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

package io.renku.triplesgenerator.config

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.generators.VersionGenerators.compatibilityGen
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class VersionCompatibilityConfigSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "apply" should {

    "fail if there are not value set for the matrix" in new TestCase {

      val config    = ConfigFactory.parseString("compatibility { }")
      val exception = intercept[Exception](readCompatibilityConfig(config).unsafeRunSync())

      exception            shouldBe a[Exception]
      exception.getMessage shouldBe "String: 1: No configuration setting found for key 'cli-version'"
    }

    "return a Compatibility config if the value is set" in new TestCase {
      val c = compatibilityGen.generateOne.copy(renkuDevVersion = None)

      val unparsedConfigElements =
        s"""
           |compatibility {
           |  cli-version = "${c.cliVersion.value}"
           |  schema-version = "${c.schemaVersion.value}"
           |  re-provisioning-needed = ${c.reProvisioningNeeded}
           |}
           |""".stripMargin

      val config = ConfigFactory.parseString(unparsedConfigElements)

      readCompatibilityConfig(config).unsafeRunSync() shouldBe c
    }

    "return a compatibility config with the RenkuDevVersion if it exists and log a warning" in new TestCase {
      val compatConfig = compatibilityGen.suchThat(_.renkuDevVersion.isDefined).generateOne

      val unparsedConfigElements =
        s"""
           |compatibility {
           |  cli-version = "${compatConfig.configuredCliVersion.value}"
           |  schema-version = "${compatConfig.schemaVersion.value}"
           |  re-provisioning-needed = ${compatConfig.reProvisioningNeeded}
           |}
           |renku-python-dev-version = "${compatConfig.renkuDevVersion.get.version}"
           |""".stripMargin

      val config = ConfigFactory.parseString(unparsedConfigElements)

      VersionCompatibilityConfig.fromConfigF[IO](config).unsafeRunSync() shouldBe compatConfig

      logger.loggedOnly(
        Warn(
          s"RENKU_PYTHON_DEV_VERSION env variable is set. CLI config version is now set to ${compatConfig.renkuDevVersion.get.version}"
        )
      )
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    def readCompatibilityConfig(config: Config): IO[VersionCompatibilityConfig] =
      VersionCompatibilityConfig.fromConfigF[IO](config)

    implicit lazy val renkuPythonDevVersions: Gen[RenkuPythonDevVersion] =
      cliVersions map (v => RenkuPythonDevVersion(v.show))
  }
}
