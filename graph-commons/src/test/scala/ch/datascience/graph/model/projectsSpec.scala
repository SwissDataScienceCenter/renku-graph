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

package ch.datascience.graph.model

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects.{FilePath, FullProjectPath, ProjectPath}
import ch.datascience.tinytypes.RelativePathTinyType
import ch.datascience.tinytypes.constraints.RelativePath
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Success, Try}

class ProjectPathSpec extends WordSpec with ScalaCheckPropertyChecks {

  "ProjectPath" should {

    "be a RelativePath" in {
      ProjectPath shouldBe a[RelativePath]
    }

    "be a RelativePathTinyType" in {
      ProjectPath(relativePaths(minSegments = 2, maxSegments = 2).generateOne) shouldBe a[RelativePathTinyType]
    }
  }

  "apply" should {

    "successfully instantiate from values having two segments separated with a '/'" in {
      forAll(relativePaths(minSegments = 2, maxSegments = 2)) { path =>
        ProjectPath(path).value shouldBe path
      }
    }

    "fail single segment values" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(nonEmptyStrings().generateOne).value
      }
    }

    "fail for values with more than two segments" in {
      forAll(relativePaths(minSegments = 3, maxSegments = 11)) { path =>
        an[IllegalArgumentException] shouldBe thrownBy {
          ProjectPath(path).value
        }
      }
    }
  }
}

class FullProjectPathSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "be instantiatable for a valid project url" in {
      forAll(httpUrls, projectPaths) { (url, path) =>
        FullProjectPath(s"$url/projects/$path").toString shouldBe s"$url/projects/$path"
      }
    }

    "throw an IllegalArgumentException for relative project paths" in {
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
        FullProjectPath(s"$url/projects/$projectPath").as[Try, ProjectPath] shouldBe Success(projectPath)
      }
    }
  }
}

class FilePathSpec extends WordSpec with ScalaCheckPropertyChecks {

  "FilePath" should {

    "be a RelativePath" in {
      FilePath shouldBe a[RelativePath]
    }
  }

  "apply" should {

    "successfully instantiate from values having at least one segment" in {
      forAll(relativePaths()) { path =>
        FilePath(path).value shouldBe path
      }
    }
  }
}
