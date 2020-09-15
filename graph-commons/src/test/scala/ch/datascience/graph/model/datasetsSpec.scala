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

import GraphModelGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.datasets._
import ch.datascience.tinytypes.UrlTinyType
import ch.datascience.tinytypes.constraints.{NonBlank, RelativePath}
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class datasetsSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

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

  "SameAs" should {

    "be a UrlTinyType" in {
      datasetSameAs.generateOne shouldBe a[UrlTinyType]
    }

    "allow to construct UrlSameAs using the from factory" in {
      forAll(httpUrls()) { url =>
        val Right(sameAs) = SameAs.from(url)
        sameAs       should (be(a[SameAs]) and be(a[UrlSameAs]))
        sameAs.value shouldBe url
      }
    }

    "allow to construct IdSameAs using the fromId factory" in {
      forAll(httpUrls()) { url =>
        val Right(sameAs) = SameAs.fromId(url)
        sameAs       should (be(a[SameAs]) and be(a[IdSameAs]))
        sameAs.value shouldBe url
      }
    }
  }

  "SameAs.equals" should {

    "return true for two SameAs having equal value regardless of the type" in {
      forAll { sameAs: SameAs =>
        SameAs.fromId(sameAs.value) shouldBe SameAs.from(sameAs.value)
        SameAs.from(sameAs.value)   shouldBe SameAs.fromId(sameAs.value)
      }
    }
  }

  "SameAs.hashCode" should {

    "return same values for two SameAs having equal value regardless of the type" in {
      forAll { sameAs: SameAs =>
        SameAs.fromId(sameAs.value).map(_.hashCode()) shouldBe SameAs.from(sameAs.value).map(_.hashCode())
      }
    }
  }

  "SameAs.fromId" should {

    "return an instance of IdSameAs" in {
      val sameAs = datasetSameAs.generateOne

      val Right(instance) = SameAs.fromId(sameAs.value)

      instance       shouldBe an[IdSameAs]
      instance.value shouldBe sameAs.value
    }
  }

  "SameAs.from" should {

    "return an instance of UrlSameAs" in {
      val sameAs = datasetSameAs.generateOne

      val Right(instance) = SameAs.from(sameAs.value)

      instance       shouldBe an[UrlSameAs]
      instance.value shouldBe sameAs.value
    }
  }

  "SameAs.apply(EntityId)" should {

    "return an instance of IdSameAs" in {
      val entityId = EntityId.of(httpUrls().generateOne)

      val instance = SameAs(entityId)

      instance       shouldBe an[IdSameAs]
      instance.value shouldBe entityId.value
    }
  }

  "SameAs jsonLdEncoder" should {

    import SameAs._

    "serialise IdSameAs to an object having url property linked to the SameAs's value" in {
      val sameAs = datasetIdSameAs.generateOne

      val json = sameAsJsonLdEncoder(sameAs).toJson

      json.hcursor.downField("@type").as[String]                                    shouldBe Right((schema / "URL").toString)
      json.hcursor.downField((schema / "url").toString).downField("@id").as[String] shouldBe Right(sameAs.toString)
    }

    "serialise UrlSameAs to an object having url property as the SameAs's value" in {
      val sameAs = datasetUrlSameAs.generateOne

      val json = sameAsJsonLdEncoder(sameAs).toJson

      json.hcursor.downField("@type").as[String]                                       shouldBe Right((schema / "URL").toString)
      json.hcursor.downField((schema / "url").toString).downField("@value").as[String] shouldBe Right(sameAs.toString)
    }
  }

  "TopmostSameAs jsonLdEncoder" should {

    import TopmostSameAs._

    "serialise TopmostSameAs to an object having @id property as the SameAs's value" in {
      val sameAs = datasetTopmostSameAs.generateOne

      val json = topmostSameAsJsonLdEncoder(sameAs).toJson

      json.hcursor.downField("@id").as[String] shouldBe Right(sameAs.toString)
    }
  }

  "derivedFrom jsonLdEncoder" should {

    import DerivedFrom._

    "serialise derivedFrom to an object having @id property linked to the DerivedFrom's value" in {
      val derivedFrom = datasetDerivedFroms.generateOne

      val json = derivedFromJsonLdEncoder(derivedFrom).toJson

      json.hcursor.downField("@id").as[String] shouldBe Right(derivedFrom.toString)
    }
  }
}
