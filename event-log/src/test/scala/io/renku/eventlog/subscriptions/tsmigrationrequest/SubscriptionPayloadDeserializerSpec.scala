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

package io.renku.eventlog.subscriptions.tsmigrationrequest

import Generators._
import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.events.consumers.subscriptions.{subscriberIds, subscriberUrls}
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonBlankStrings
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class SubscriptionPayloadDeserializerSpec extends AnyWordSpec with should.Matchers {

  "deserialize" should {

    "return subscription info if the categoryName, subscriber url and version are valid" in {
      val subscription = migratorSubscriptionInfos.generateOne

      val payload = json"""{
          "categoryName": "TS_MIGRATION_REQUEST",
          "subscriber": {
            "url":     ${subscription.subscriberUrl.value},
            "id":      ${subscription.subscriberId.value},
            "version": ${subscription.subscriberVersion.value}
          }
        }"""

      deserializer.deserialize(payload) shouldBe Success(Some(subscription))
    }

    "return None if the payload does not contain a valid categoryName" in {

      val payload = json"""{
        "categoryName":  ${nonBlankStrings().generateOne.value},
          "subscriber": {
            "url":     ${subscriberUrls.generateOne.value},
            "id":      ${subscriberIds.generateOne.value},
            "version": ${serviceVersions.generateOne.value}
          }
      }"""

      deserializer.deserialize(payload) shouldBe Option.empty[MigratorSubscriptionInfo].pure[Try]
    }

    "return None if the payload does not contain required fields" in {
      deserializer.deserialize(Json.obj()) shouldBe Option.empty[MigratorSubscriptionInfo].pure[Try]
    }
  }

  private lazy val deserializer = new SubscriptionPayloadDeserializer[Try]()
}
