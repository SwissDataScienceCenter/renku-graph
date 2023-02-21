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

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.Generators.{capacitySubscribers, noCapacitySubscribers, subscriptionPayloads}
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues

class DefaultSubscriptionSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "encoder" should {

    "produce json from SubscriptionPayload with subscriber without capacity" in {

      val subscriber          = noCapacitySubscribers.generateOne
      val subscriptionPayload = subscriptionPayloads.generateOne.copy(subscriber = subscriber)

      subscriptionPayload.asJson shouldBe json"""{
        "categoryName": ${subscriptionPayload.categoryName},
        "subscriber": {
          "url": ${subscriber.url},
          "id":  ${subscriber.id}
        }
      }"""
    }

    "produce json from SubscriptionPayload with subscriber with capacity" in {

      val subscriber          = capacitySubscribers.generateOne
      val subscriptionPayload = subscriptionPayloads.generateOne.copy(subscriber = subscriber)

      subscriptionPayload.asJson shouldBe json"""{
        "categoryName": ${subscriptionPayload.categoryName},
        "subscriber": {
          "url":      ${subscriber.url},
          "id":       ${subscriber.id},
          "capacity": ${subscriber.capacity}
        }
      }"""
    }
  }

  "decoder" should {

    "be able to decode encoded SubscriptionPayload with subscriber without capacity" in {

      val subscriber          = noCapacitySubscribers.generateOne
      val subscriptionPayload = subscriptionPayloads.generateOne.copy(subscriber = subscriber)

      subscriptionPayload.asJson.hcursor.as[DefaultSubscription].value shouldBe subscriptionPayload
    }

    "be able to decode encoded SubscriptionPayload with subscriber with capacity" in {

      val subscriber          = capacitySubscribers.generateOne
      val subscriptionPayload = subscriptionPayloads.generateOne.copy(subscriber = subscriber)

      subscriptionPayload.asJson.hcursor.as[DefaultSubscription].value shouldBe subscriptionPayload
    }
  }

  "show" should {

    "return only the url and id for no capacity subscriber" in {
      val subscriber = noCapacitySubscribers.generateOne
      subscriber.show shouldBe s"subscriber = ${subscriber.url}, id = ${subscriber.id}"
    }

    "return the url, id and capacity when it's present" in {
      val subscriber = capacitySubscribers.generateOne
      subscriber.show shouldBe s"subscriber = ${subscriber.url}, id = ${subscriber.id}, capacity = ${subscriber.capacity}"
    }
  }
}
