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

package ch.datascience.config

import java.net.MalformedURLException

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

class ServiceUrlSpec extends WordSpec with PropertyChecks {

  "apply" should {

    "be successful for valid urls" in {
      forAll(httpUrls) { url =>
        ServiceUrl(url).toString shouldBe url
      }
    }

    "fail for invalid urls" in {
      an[MalformedURLException] should be thrownBy {
        ServiceUrl("invalid url")
      }
    }
  }

  "/" should {

    "allow to add next path part" in {
      val url = serviceUrls.generateOne
      (url / "path").toString shouldBe s"$url/path"
    }
  }

  "from" should {

    "instantiate for valid urls" in {
      val serviceUrl = serviceUrls.generateOne
      ServiceUrl.from(serviceUrl.toString) shouldBe Right(serviceUrl)
    }

    "fail with IllegalArgumentException for invalid urls" in {
      val Left(failure) = ServiceUrl.from("dfh://asdf")
      failure.getMessage shouldBe s"Cannot instantiate ${ServiceUrl.getClass.getName}".replace("$", "")
    }
  }
}
