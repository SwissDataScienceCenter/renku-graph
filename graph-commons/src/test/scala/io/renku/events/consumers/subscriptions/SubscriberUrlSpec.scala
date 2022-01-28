/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.events.consumers.subscriptions

import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{httpUrls, nonBlankStrings, relativePaths}
import io.renku.microservices.MicroserviceBaseUrl
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SubscriberUrlSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "compose the SubscriberUrl from the given MicroserviceBaseUrl and '/events'" in {

      val baseUrl = microserviceBaseUrls.generateOne
      val part    = nonBlankStrings().generateOne

      SubscriberUrl(baseUrl, part).value shouldBe (baseUrl / part.value).toString
    }
  }

  "to MicroserviceBaseUrl conversion" should {

    "successfully convert MicroserviceBaseUrl if well defined" in {
      forAll(httpUrls(pathGenerator = Gen.const("")), relativePaths(minSegments = 0, maxSegments = 2)) { (url, path) =>
        val pathValidated = if (path.isEmpty) "" else s"/$path"
        SubscriberUrl(s"$url$pathValidated").toUnsafe[MicroserviceBaseUrl] shouldBe MicroserviceBaseUrl(url)
      }
    }
  }
}
