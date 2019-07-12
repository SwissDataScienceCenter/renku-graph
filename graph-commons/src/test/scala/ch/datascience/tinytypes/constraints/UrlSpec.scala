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

package ch.datascience.tinytypes.constraints

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.httpUrls
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UrlSpec extends WordSpec with ScalaCheckPropertyChecks {

  "Url" should {

    "be instantiatable for valid urls" in {
      forAll(httpUrls) { url =>
        UrlType(url).toString shouldBe url
      }
    }

    "throw an IllegalArgumentException for invalid urls" in {
      intercept[IllegalArgumentException](UrlType("invalid url")).getMessage shouldBe "Cannot instantiate ch.datascience.tinytypes.constraints.UrlType with 'invalid url'"
    }
  }

  "/" should {

    "allow to add next path part" in {
      val url = (httpUrls map UrlType.apply).generateOne
      (url / "path").toString shouldBe s"$url/path"
    }
  }
}

private class UrlType private (val value: String) extends AnyVal with StringTinyType

private object UrlType extends TinyTypeFactory[UrlType](new UrlType(_)) with Url with UrlOps[UrlType]
