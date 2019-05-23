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

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.constraints.Url
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ServiceUrlSpec extends WordSpec with ScalaCheckPropertyChecks {

  "ServiceUrl" should {

    "have the Url constraint" in {
      ServiceUrl shouldBe an[Url]
    }
  }

  "/" should {

    "allow to add next path part" in {
      val url = serviceUrls.generateOne
      (url / "path").toString shouldBe s"$url/path"
    }
  }

  "equal" should {

    "return true when representing the same url" in {
      ServiceUrl("http://localhost:9000/abc") shouldBe ServiceUrl("http://localhost:9000/abc")
    }

    "return false when representing different url" in {
      ServiceUrl("http://localhost:9000/abcs") should not be ServiceUrl("http://localhost:9000/abc")
    }

    "return true when representing the same url and having the same pre-host clauses" in {
      ServiceUrl("http://key:value@localhost:9000/abc") shouldBe ServiceUrl(
        "http://key:value@localhost:9000/abc"
      )
    }

    "return false when representing the same url but having different pre-host clauses" in {
      ServiceUrl("http://key:value1@localhost:9000/abc") should not be ServiceUrl(
        "http://key:value2@localhost:9000/abc"
      )
    }
  }
}
