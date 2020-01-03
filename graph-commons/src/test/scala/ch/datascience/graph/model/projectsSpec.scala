/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects.{ProjectPath, ProjectResource}
import ch.datascience.tinytypes.constraints.{RelativePath, Url}
import eu.timepit.refined.auto._
import org.scalacheck.Gen.{alphaChar, const, frequency, numChar, oneOf}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectPathSpec extends WordSpec with ScalaCheckPropertyChecks {

  "ProjectPath" should {

    "be a RelativePath" in {
      ProjectPath shouldBe a[RelativePath]
    }
  }

  "instantiation" should {

    "be successful for relative paths with min number of 2 segments" in {
      forAll(relativePaths(minSegments = 2, maxSegments = 22, partsGenerator)) { path =>
        ProjectPath(path).value shouldBe path
      }
    }

    "fail for relative paths of single segment" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(nonBlankStrings().generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(relativePaths(minSegments = 2, maxSegments = 22).generateOne + "/")
      }
    }

    "fail for absolute URLs" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectPath(httpUrls().generateOne)
      }
    }
  }

  private val partsGenerator = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"
  }
}

class ProjectResourceSpec extends WordSpec with ScalaCheckPropertyChecks {

  import GraphModelGenerators.projectPaths

  "ProjectResource" should {

    "be a RelativePath" in {
      ProjectResource shouldBe an[Url]
    }
  }

  "instantiation" should {

    "be successful for URLs ending with a project path" in {
      forAll(httpUrls(pathGenerator)) { url =>
        println(url)
        ProjectResource(url).value shouldBe url
      }
    }

    "fail for relative paths" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectResource(projectPaths.generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ProjectResource(httpUrls(pathGenerator).generateOne + "/")
      }
    }
  }

  private val pathGenerator = for {
    projectPath <- projectPaths
  } yield s"projects/$projectPath"
}
