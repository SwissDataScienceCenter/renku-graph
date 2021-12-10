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

package io.renku.graph.model

import GraphModelGenerators._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import io.circe.Json
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.datasets._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import io.renku.tinytypes.constraints.{NonBlank, RelativePath}
import io.renku.tinytypes.{RelativePathTinyType, UrlTinyType}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class datasetsSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers with Schemas {

  import SameAs._

  "ResourceId" should {
    "be renderable as RDF resource" in {
      val id = datasetResourceIds.generateOne
      id.showAs[RdfResource] shouldBe s"<${sparqlEncode(id.value)}>"
    }
  }

  "Identifier" should {
    "be a NonBlank" in {
      Identifier shouldBe a[NonBlank]
    }
  }

  "PartLocation" should {
    "be a RelativePath" in {
      PartLocation shouldBe a[RelativePath]
    }
  }

  "ImageUri" should {

    "instantiate as ImageUri.Relative for relative paths" in {
      forAll(relativePaths()) { path =>
        val uri = ImageUri(path)
        uri                 shouldBe a[ImageUri.Relative]
        uri                 shouldBe a[RelativePathTinyType]
        ImageUri.from(path) shouldBe uri.asRight
      }
    }

    "instantiate as ImageUri.Absolute for absolute paths" in {
      forAll(httpUrls()) { path =>
        val uri = ImageUri(path)
        uri                 shouldBe a[ImageUri.Absolute]
        uri                 shouldBe an[UrlTinyType]
        ImageUri.from(path) shouldBe uri.asRight
      }
    }

    "provide an implicit Json encoder" in {
      forAll(datasetImageUris) { uri =>
        uri.asJson shouldBe Json.fromString(uri.value)
      }
    }

    "provide an implicit Json decoder" in {
      forAll(datasetImageUris) { uri =>
        Json.fromString(uri.value).as[ImageUri] shouldBe uri.asRight
      }
    }
  }

  "SameAs" should {

    "be a UrlTinyType" in {
      datasetSameAs.generateOne shouldBe a[UrlTinyType]
    }

    "allow to construct ExternalSameAs using the from factory" in {
      forAll(httpUrls()) { url =>
        val Right(sameAs) = SameAs.from(url)
        sameAs         should (be(a[SameAs]) and be(a[ExternalSameAs]))
        sameAs.value shouldBe url
      }
    }

    "allow to construct InternalSameAs using the internal factory" in {
      forAll(renkuBaseUrls) { url =>
        val Right(sameAs) = SameAs.internal(url)
        sameAs         should (be(a[SameAs]) and be(a[InternalSameAs]))
        sameAs.value shouldBe url.value
      }
    }
  }

  "SameAs.equals" should {

    "return true for two SameAs having equal value regardless of the type - case of InternalSameAs" in {
      forAll { sameAs: InternalSameAs =>
        SameAs.internal(RenkuBaseUrl(sameAs.value)) shouldBe SameAs.from(sameAs.value)
        SameAs.from(sameAs.value)                   shouldBe SameAs.internal(RenkuBaseUrl(sameAs.value))
      }
    }

    "return true for two SameAs having equal value regardless of the type - case of ImportedSameAs" in {
      forAll { sameAs: ExternalSameAs =>
        SameAs.external(Refined.unsafeApply(sameAs.value)) shouldBe SameAs.from(sameAs.value)
        SameAs.from(sameAs.value)                          shouldBe SameAs.external(Refined.unsafeApply(sameAs.value))
      }
    }
  }

  "SameAs.hashCode" should {

    "return same values for two SameAs having equal value regardless of the type" in {
      forAll(datasetSameAs) { sameAs =>
        SameAs.internal(RenkuBaseUrl(sameAs.value)).map(_.hashCode()) shouldBe SameAs
          .external(Refined.unsafeApply(sameAs.value))
          .map(_.hashCode())
      }
    }
  }

  "SameAs.internal" should {

    "return an instance of InternalSameAs" in {
      val sameAs = datasetInternalSameAs.generateOne

      val Right(instance) = SameAs.internal(RenkuBaseUrl(sameAs.value))

      instance       shouldBe an[InternalSameAs]
      instance.value shouldBe sameAs.value
    }
  }

  "SameAs.from" should {

    "return an instance of UrlSameAs" in {
      val sameAs = datasetExternalSameAs.generateOne

      val Right(instance) = SameAs.from(sameAs.value)

      instance       shouldBe an[ExternalSameAs]
      instance.value shouldBe sameAs.value
    }
  }

  "SameAs.apply(EntityId)" should {

    "return an instance of IdSameAs" in {
      val entityId = EntityId.of(httpUrls().generateOne)

      val instance = SameAs(entityId)

      instance       shouldBe an[InternalSameAs]
      instance.value shouldBe entityId.value
    }
  }

  "SameAs jsonLdEncoder" should {

    "serialise IdSameAs to an object having url property linked to the SameAs's value" in {
      val sameAs = datasetInternalSameAs.generateOne

      val json = sameAs.asJsonLD.toJson

      json.hcursor.downField("@type").as[String] shouldBe Right((schema / "URL").toString)
      json.hcursor.downField((schema / "url").toString).downField("@id").as[String] shouldBe Right(sameAs.toString)
    }

    "serialise UrlSameAs to an object having url property as the SameAs's value" in {
      val sameAs = datasetExternalSameAs.generateOne

      val json = sameAs.asJsonLD.toJson

      json.hcursor.downField("@type").as[String] shouldBe Right((schema / "URL").toString)
      json.hcursor.downField((schema / "url").toString).downField("@value").as[String] shouldBe Right(sameAs.toString)
    }
  }

  "TopmostSameAs jsonLdEncoder" should {

    "serialise TopmostSameAs to an object having @id property as the SameAs's value" in {
      val sameAs = datasetTopmostSameAs.generateOne

      val json = TopmostSameAs.topmostSameAsJsonLdEncoder(sameAs).toJson

      json.hcursor.downField("@id").as[String] shouldBe Right(sameAs.toString)
    }
  }

  "derivedFrom jsonLdEncoder" should {

    "serialise derivedFrom to an object of type URL having schema:url property linked to the DerivedFrom's value" in {
      val derivedFrom = datasetDerivedFroms.generateOne

      val json = DerivedFrom.jsonLDEncoder(derivedFrom).toJson

      json.hcursor.downField("@type").as[String] shouldBe Right((schema / "URL").toString)
      json.hcursor.downField((schema / "url").toString).downField("@id").as[String] shouldBe Right(derivedFrom.toString)
    }
  }

  "TopmostDerivedFrom jsonLdEncoder" should {

    "serialise TopmostDerivedFrom to an object having @id property as the DerivedFrom's value" in {
      val derivedFrom = datasetTopmostDerivedFroms.generateOne

      val json = TopmostDerivedFrom.topmostDerivedFromJsonLdEncoder(derivedFrom).toJson

      json.hcursor.downField("@id").as[String] shouldBe Right(derivedFrom.toString)
    }
  }
}
