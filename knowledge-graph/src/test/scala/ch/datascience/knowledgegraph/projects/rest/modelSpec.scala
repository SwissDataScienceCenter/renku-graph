/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.projects.rest

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.projects.model.RepoUrls._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class modelSpec extends WordSpec with ScalaCheckPropertyChecks {

  "HttpUrl" should {

    "instantiate for valid absolute git urls" in {
      forAll(httpUrls, projectPaths) { (httpUrl, projectPath) =>
        val url = s"$httpUrl/$projectPath.git"
        HttpUrl.from(url).map(_.value) shouldBe Right(url)
      }
    }

    "fail instantiation for non-absolute urls" in {
      val url = s"${relativePaths().generateOne}/${projectPaths.generateOne}.git"

      val Left(exception) = HttpUrl.from(url)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"$url is not a valid repository http url"
    }
  }

  "SshUrl" should {

    "instantiate for valid absolute ssh urls" in {
      forAll(nonEmptyList(nonBlankStrings()), projectPaths) { (hostParts, projectPath) =>
        val url = s"git@${hostParts.toList.mkString(".")}:$projectPath.git"
        SshUrl.from(url).map(_.value) shouldBe Right(url)
      }
    }

    "fail instantiation for non-ssh urls" in {
      val url = s"${gitLabUrls.generateOne}/${projectPaths.generateOne}.git"

      val Left(exception) = SshUrl.from(url)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"$url is not a valid repository ssh url"
    }
  }
}
