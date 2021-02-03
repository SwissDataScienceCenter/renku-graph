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

package io.renku.eventlog.subscriptions

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators._
import ch.datascience.generators.Generators.Implicits._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SubscriptionInfoSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "equal" should {

    "return true if both infos have the same subscriberUrls" in {
      val subscriberUrl = subscriberUrls.generateOne
      forAll(subscriptionInfos, subscriptionInfos) { (info1, info2) =>
        info1.copy(subscriberUrl = subscriberUrl) shouldBe info2.copy(subscriberUrl = subscriberUrl)
      }
    }

    "return false if both infos have different subscriberUrls" in {
      val subscriberUrl1 = subscriberUrls.generateOne
      val subscriberUrl2 = subscriberUrls.generateOne
      val info           = subscriptionInfos.generateOne
      info.copy(subscriberUrl1) should not be info.copy(subscriberUrl2)
    }
  }

  "toString" should {

    "return only the url if no capacity is present" in {
      val info = subscriptionInfos.generateOne.copy(maybeCapacity = None)
      info.toString shouldBe info.subscriberUrl.toString
    }

    "return the url with capacity when it's present" in {
      val capacity = capacities.generateOne
      val info     = subscriptionInfos.generateOne.copy(maybeCapacity = Some(capacity))
      info.toString shouldBe s"${info.subscriberUrl} with capacity $capacity"
    }
  }
}
