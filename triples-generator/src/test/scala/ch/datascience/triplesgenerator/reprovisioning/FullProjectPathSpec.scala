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

package ch.datascience.triplesgenerator.reprovisioning

import ReProvisioningGenerators._
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpUrls
import ch.datascience.graph.model.events.EventsGenerators.projectPaths
import ch.datascience.graph.model.events.ProjectPath
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Success, Try}

class FullProjectPathSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "be instantiatable for a valid project url" in {
      forAll(httpUrls, projectPaths) { (url, path) =>
        FullProjectPath(s"$url/$path").toString shouldBe s"$url/$path"
      }
    }

    "throw an IllegalArgumentException for paths" in {
      val path = projectPaths.generateOne
      intercept[Exception] {
        FullProjectPath(path.value)
      } shouldBe an[IllegalArgumentException]
    }

    "throw an IllegalArgumentException for a blank value" in {
      intercept[Exception] {
        FullProjectPath("   ")
      } shouldBe an[IllegalArgumentException]
    }
  }

  "toProjectPath" should {

    "return a valid ProjectPath" in {
      forAll(httpUrls, projectPaths) { (url, projectPath) =>
        FullProjectPath(s"$url/$projectPath").to[Try, ProjectPath] shouldBe Success(projectPath)
      }
    }
  }

  "rdfResourceRenderer" should {

    "wrap the value into <>" in {
      val projectPath = fullProjectPaths.generateOne
      projectPath.showAs[RdfResource] shouldBe s"<$projectPath>"
    }
  }
}
