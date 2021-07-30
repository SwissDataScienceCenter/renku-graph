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

package ch.datascience.graph.model

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects._
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.tinytypes.constraints.{RelativePath, Url}
import eu.timepit.refined.auto._
import io.circe.{DecodingFailure, Json}
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, const, frequency, numChar}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class IdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "instantiation" should {

    "be successful for non-negative values" in {
      forAll(nonNegativeInts()) { id =>
        Id(id.value).value shouldBe id.value
      }
    }

    "fail for negative ids" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Id(-1).value
      }
    }
  }
}

class PathSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "Path" should {
    "be a RelativePath" in {
      Path shouldBe a[RelativePath]
    }
  }

  "instantiation" should {

    "be successful for relative paths with min number of 2 segments" in {
      forAll(relativePaths(minSegments = 2, maxSegments = 22, partsGenerator)) { path =>
        Path(path).value shouldBe path
      }
    }

    "fail for relative paths of single segment" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Path(nonBlankStrings().generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Path(relativePaths(minSegments = 2, maxSegments = 22).generateOne + "/")
      }
    }

    "fail for absolute URLs" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Path(httpUrls().generateOne)
      }
    }
  }

  "toName" should {
    "extract the very last path segment" in {
      forAll(projectNamespaces.toGeneratorOfNonEmptyList(), projectNames) { (namespaces, name) =>
        Path(s"${namespaces.map(_.show).nonEmptyIntercalate("/")}/$name").toName shouldBe name
      }
    }
  }

  "toNamespaces" should {
    "extract all the path segment except the last" in {
      forAll(projectNamespaces.toGeneratorOfNonEmptyList(), projectNames) { (namespaces, name) =>
        Path(s"${namespaces.map(_.show).nonEmptyIntercalate("/")}/$name").toNamespaces shouldBe namespaces.toList
      }
    }
  }

  private lazy val partsGenerator = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> Gen.oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"
  }
}

class VisibilitySpec extends AnyWordSpec with should.Matchers {

  "Visibility" should {

    "define cases for 'private', 'public' and 'internal'" in {
      Visibility.all.map(_.value) should contain.only("private", "public", "internal")
    }
  }

  "projectVisibilityDecoder" should {

    Visibility.all foreach { visibility =>
      s"deserialize $visibility" in {
        Json.fromString(visibility.value).as[Visibility] shouldBe Right(visibility)
      }
    }

    "fail for unknown value" in {
      Json.fromString("unknown").as[Visibility] shouldBe Left(
        DecodingFailure(
          s"'unknown' is not a valid project visibility. Allowed values are: ${Visibility.all.mkString(", ")}",
          Nil
        )
      )
    }
  }
}

class ResourceIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "ResourceId" should {

    "be a RelativePath" in {
      ResourceId shouldBe an[Url]
    }
  }

  "instantiation" should {

    "be successful for URLs ending with a project path" in {
      forAll(httpUrls(pathGenerator)) { url =>
        ResourceId(url).value shouldBe url
      }
    }

    "fail for relative paths" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ResourceId(projectPaths.generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ResourceId(httpUrls(pathGenerator).generateOne + "/")
      }
    }
  }

  "toProjectPath converter" should {

    "convert any Project Resource to ProjectPath" in {
      forAll { (renkuBaseUrl: RenkuBaseUrl, projectPath: Path) =>
        ResourceId(renkuBaseUrl, projectPath).as[Try, Path] shouldBe projectPath.pure[Try]
      }
    }
  }

  "showAs[RdfResource]" should {

    "wrap the ResourceId in <>" in {
      forAll { resourceId: ResourceId =>
        resourceId.showAs[RdfResource] shouldBe s"<${resourceId.value}>"
      }
    }
  }

  private lazy val pathGenerator = for {
    projectPath <- projectPaths
  } yield s"projects/$projectPath"
}
