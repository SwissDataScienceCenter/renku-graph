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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest

import cats.syntax.all._
import io.circe.literal._
import io.renku.events.consumers.subscriptions.subscriberUrls
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.microservices.MicroserviceIdentifier
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class PayloadComposerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "prepareSubscriptionPayload" should {

    "return Payload containing the given CategoryName, found subscriberUrl and capacity" in new TestCase {
      composer.prepareSubscriptionPayload() shouldBe json"""{
        "categoryName" : ${categoryName.value},
        "subscriber": {
          "url":     ${subscriberUrl.value},
          "id":      ${serviceId.value},
          "version": ${serviceVersion.value}
        }
      }""".pure[Try]
    }
  }

  private trait TestCase {
    val subscriberUrl  = subscriberUrls.generateOne
    val serviceId      = MicroserviceIdentifier.generate
    val serviceVersion = serviceVersions.generateOne
    val composer       = new PayloadComposer[Try](subscriberUrl, serviceId, serviceVersion)
  }
}
