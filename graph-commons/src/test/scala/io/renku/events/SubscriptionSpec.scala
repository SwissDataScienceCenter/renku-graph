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

package io.renku.events

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators._
import io.renku.generators.Generators.Implicits._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SubscriptionSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "equals" should {

    "return true if both subscribers have the same urls" in {

      val url = subscriberUrls.generateOne
      val id  = subscriberIds.generateOne

      forAll(subscribers, subscribers) { (subscriber1, subscriber2) =>
        subscriber1.fold(_.copy(url = url, id = id), _.copy(url = url, id = id)) shouldBe
          subscriber2.fold(_.copy(url = url, id = id), _.copy(url = url, id = id))
      }
    }

    "return true if both subscribers have the same urls but different ids" in {

      val url        = subscriberUrls.generateOne
      val subscriber = subscribers.generateOne

      subscriber
        .fold(_.copy(url = url, id = subscriberIds.generateOne),
              _.copy(url = url, id = subscriberIds.generateOne)
        ) shouldBe
        subscriber.fold(_.copy(url = url, id = subscriberIds.generateOne),
                        _.copy(url = url, id = subscriberIds.generateOne)
        )
    }

    "return false if both subscribers have different urls" in {

      val url1       = subscriberUrls.generateOne
      val url2       = subscriberUrls.generateOne
      val subscriber = subscribers.generateOne

      subscriber.fold(_.copy(url = url1), _.copy(url = url1)) should not be
        subscriber.fold(_.copy(url = url2), _.copy(url = url2))
    }
  }
}
