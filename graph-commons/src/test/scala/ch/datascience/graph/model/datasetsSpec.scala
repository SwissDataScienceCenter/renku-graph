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
  }

  "SameAs.apply(EntityId)" should {

    "return an instance of IdSameAs" in {
      val entityId = EntityId.of(httpUrls().generateOne)
      SameAs(entityId).value shouldBe entityId.value
    }
  }

  "SameAs jsonLdEncoder" should {

    import SameAs._

    "serialise SameAs to an object having url property linked to the SameAs's value" in {
      val sameAs = datasetSameAs.generateOne

      val json = sameAsJsonLdEncoder(sameAs).toJson

      json.hcursor.downField("@type").as[String]                                    shouldBe Right((schema / "URL").toString)
      json.hcursor.downField((schema / "url").toString).downField("@id").as[String] shouldBe Right(sameAs.toString)
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
