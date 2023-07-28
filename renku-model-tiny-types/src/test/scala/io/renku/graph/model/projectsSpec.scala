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

package io.renku.graph.model

import cats.syntax.all._
import io.circe.{DecodingFailure, Json}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.projects._
import io.renku.graph.model.views.RdfResource
import io.renku.tinytypes.constraints.{RelativePath, Url}
import org.apache.jena.util.URIref
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, const, frequency, numChar}
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class ProjectGitLabIdSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "instantiation" should {

    "be successful for non-negative values" in {
      forAll(nonNegativeInts()) { id =>
        GitLabId(id.value).value shouldBe id.value
      }
    }

    "fail for negative ids" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        GitLabId(-1).value
      }
    }
  }
}

class SlugSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers with RenkuTinyTypeGenerators {

  "Path" should {
    "be a RelativePath" in {
      Slug shouldBe a[RelativePath[_]]
    }
  }

  "instantiation" should {

    "be successful for relative paths with min number of 2 segments" in {
      forAll(relativePaths(minSegments = 2, maxSegments = 22, partsGenerator)) { path =>
        Slug(path).value shouldBe path
      }
    }

    "fail for relative paths of single segment" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Slug(nonBlankStrings().generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Slug(relativePaths(minSegments = 2, maxSegments = 22).generateOne + "/")
      }
    }

    "fail for absolute URLs" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        Slug(httpUrls().generateOne)
      }
    }
  }

  "toName" should {
    "extract the very last path segment" in {
      forAll(projectNamespaces.toGeneratorOfNonEmptyList(), projectNames) { (namespaces, name) =>
        Slug(s"${namespaces.map(_.show).nonEmptyIntercalate("/")}/$name").toName shouldBe name
      }
    }
  }

  "toNamespaces" should {
    "extract all the slug segments except the last one" in {
      forAll(projectNamespaces.toGeneratorOfNonEmptyList(), projectNames) { (namespaces, name) =>
        Slug(s"${namespaces.map(_.show).nonEmptyIntercalate("/")}/$name").toNamespaces shouldBe namespaces.toList
      }
    }
  }

  "toNamespace" should {
    "extract the namespace from the slug" in {
      forAll(projectNamespaces.toGeneratorOfNonEmptyList(), projectNames) { (namespaces, name) =>
        val namespaceAsString = namespaces.map(_.show).nonEmptyIntercalate("/")
        Slug(s"$namespaceAsString/$name").toNamespace shouldBe Namespace(namespaceAsString)
      }
    }
  }

  private lazy val partsGenerator = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> Gen.oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, min = 5, max = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"
  }
}

class VisibilitySpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {

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

  "ordering" should {

    forAll {
      Table(
        ("in", "out", "result"),
        (Visibility.Public, Visibility.Public, 0),
        (Visibility.Public, Visibility.Internal, 1),
        (Visibility.Public, Visibility.Private, 2),
        (Visibility.Internal, Visibility.Public, -1),
        (Visibility.Internal, Visibility.Internal, 0),
        (Visibility.Internal, Visibility.Private, 1),
        (Visibility.Private, Visibility.Public, -2),
        (Visibility.Private, Visibility.Internal, -1),
        (Visibility.Private, Visibility.Private, 0)
      )
    } { (in, out, result) =>
      show"return $result for $in compare to $out" in {
        in.compareTo(out)                                 shouldBe result
        implicitly[Ordering[Visibility]].compare(in, out) shouldBe result
      }
    }
  }
}

class ProjectResourceIdSpec
    extends AnyWordSpec
    with ScalaCheckPropertyChecks
    with should.Matchers
    with RenkuTinyTypeGenerators {

  "ResourceId" should {

    "be a RelativePath" in {
      ResourceId shouldBe an[Url[ResourceId]]
    }
  }

  "instantiation" should {

    "be successful for URLs ending with a project slug" in {
      forAll(httpUrls(pathGenerator = pathGenerator)) { url =>
        ResourceId(url).value shouldBe url
      }
    }

    "fail for relative paths" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ResourceId(projectSlugs.generateOne.value)
      }
    }

    "fail when ending with a /" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        ResourceId(httpUrls(pathGenerator = pathGenerator).generateOne + "/")
      }
    }
  }

  "toProjectSlug converter" should {

    "convert any Project Resource to ProjectSlug" in {
      forAll { (renkuUrl: RenkuUrl, projectSlug: Slug) =>
        ResourceId(projectSlug)(renkuUrl).as[Try, Slug] shouldBe projectSlug.pure[Try]
      }
    }
  }

  "showAs[RdfResource]" should {

    "URI encode and wrap the ResourceId in <>" in {
      forAll { resourceId: ResourceId =>
        resourceId.showAs[RdfResource] shouldBe s"<${URIref.encode(resourceId.value)}>"
      }
    }
  }

  private lazy val pathGenerator = for {
    projectPath <- projectSlugs
  } yield s"projects/$projectPath"
}
