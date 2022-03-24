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

package io.renku.eventlog.subscriptions

import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.eventlog.subscriptions.Generators._
import io.renku.events.Generators.categoryNames
import io.renku.events.consumers.subscriptions._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

private class UrlAndIdSubscriptionDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserialize" should {

    "return subscription info if the categoryName, subscriber URL and subscriber id are valid " +
      "and there's no capacity" in new TestCase {
        val subscriptionCategoryPayload = urlAndIdSubscriptionInfos.generateOne.copy(maybeCapacity = None)

        val payload = json"""{
          "categoryName":  ${categoryName.value},
          "subscriber": {
            "url": ${subscriptionCategoryPayload.subscriberUrl.value},
            "id":  ${subscriptionCategoryPayload.subscriberId.value}
          }
        }"""

        deserializer.deserialize(payload) shouldBe Success(Some(subscriptionCategoryPayload))
      }

    "return subscription info if the categoryName, subscriber URL, subscriber Id, and capacity are given and valid" in new TestCase {
      val capacity                    = capacities.generateOne
      val subscriptionCategoryPayload = urlAndIdSubscriptionInfos.generateOne.copy(maybeCapacity = capacity.some)

      val payload = json"""{
        "categoryName":  ${categoryName.value},
        "subscriber": {
          "url":      ${subscriptionCategoryPayload.subscriberUrl.value},
          "id":       ${subscriptionCategoryPayload.subscriberId.value},
          "capacity": ${capacity.value}
        }
      }"""

      deserializer.deserialize(payload) shouldBe Success(Some(subscriptionCategoryPayload))
    }

    "return None if the payload does not contain a valid categoryName" in new TestCase {

      val payload = json"""{
        "categoryName":  ${nonBlankStrings().generateOne.value},
          "subscriber": {
            "url": ${subscriberUrls.generateOne.value},
            "id":  ${subscriberIds.generateOne.value}
          }
      }"""

      deserializer.deserialize(payload) shouldBe Success(Option.empty[TestSubscriptionInfo])
    }

    "return None if the payload does not contain required fields" in new TestCase {
      deserializer.deserialize(Json.obj()) shouldBe Success(Option.empty[TestSubscriptionInfo])
    }
  }

  private trait TestCase {
    val categoryName = categoryNames.generateOne
    val Success(deserializer) = UrlAndIdSubscriptionDeserializer[Try, TestSubscriptionInfo](
      categoryName,
      TestSubscriptionInfo.apply
    )
  }
}
