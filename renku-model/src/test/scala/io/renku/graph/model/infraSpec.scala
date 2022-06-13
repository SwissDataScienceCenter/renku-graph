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

package io.renku.graph.model

import GraphModelGenerators.cliVersions
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliVersionSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "from" should {

    "return Right for a valid released version" in {
      forAll(semanticVersions) { version =>
        CliVersion.from(version).map(_.show) shouldBe version.asRight
      }
    }

    "return Right for a valid dev version" in {
      forAll(devVersions()) { version =>
        CliVersion.from(version).map(_.show) shouldBe version.asRight
      }
    }
  }

  "major, minor and bugfix" should {

    "extract the relevant version parts from a released version" in {
      forAll(semanticVersions) { version =>
        val cli                      = CliVersion(version)
        val s"$major.$minor.$bugfix" = version
        cli.major  shouldBe major
        cli.minor  shouldBe minor
        cli.bugfix shouldBe bugfix
      }
    }

    "extract the relevant version parts from a dev version" in {
      forAll(devVersions()) { version =>
        val cli                      = CliVersion(version)
        val s"$major.$minor.$bugfix" = version
        cli.major  shouldBe major
        cli.minor  shouldBe minor
        cli.bugfix shouldBe bugfix
      }
    }
  }

  "ordering" should {

    "consider the major only if different" in {
      forAll(cliVersions, cliVersions) { (version1, version2) =>
        whenever(version1.major != version2.major) {
          val list = List(version1, version2)

          if (version1.major < version2.major) list.sorted shouldBe list
          else list.sorted                                 shouldBe list.reverse
        }
      }
    }

    "consider the minor only if majors are the same" in {
      forAll(cliVersions, cliVersions) { (version1, version2) =>
        val version2SameMajor = CliVersion(version2.show.replaceFirst(s"${version2.major}.", s"${version1.major}."))

        val list = List(version1, version2SameMajor)

        if (version1.minor < version2SameMajor.minor) list.sorted shouldBe list
        else list.sorted                                          shouldBe list.reverse
      }
    }

    "consider the bugfix only if majors and minors are the same" in {
      forAll(cliVersions, cliVersions) { (version1, version2) =>
        val version2SameMajorMinor = CliVersion(
          version2.show
            .replaceFirst(s"${version2.major}.${version2.minor}", s"${version1.major}.${version1.minor}")
        )

        val list = List(version1, version2SameMajorMinor)

        if (version1.bugfix < version2SameMajorMinor.bugfix) list.sorted shouldBe list
        else list.sorted                                                 shouldBe list.reverse
      }
    }

    "consider the dev part if all majors, minors and bugfix are the same" in {
      val semanticVersion = semanticVersions.generateOne
      forAll(devVersions(fixed(semanticVersion)), devVersions(fixed(semanticVersion))) { (version1, version2) =>
        val list = List(version1, version2)

        if ((version1.show compareTo version2.show) < 0) list.sorted shouldBe list
        else list.sorted                                             shouldBe list.reverse
      }
    }
  }

  private def devVersions(semanticVersionsGen: Gen[String] = semanticVersions) =
    (semanticVersionsGen, positiveInts(999), shas.map(_.take(7)))
      .mapN((version, commitsNumber, sha) => s"$version.dev$commitsNumber+g$sha")
}
