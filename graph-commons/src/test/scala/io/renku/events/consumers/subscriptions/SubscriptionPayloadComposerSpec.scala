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

import cats.syntax.all._
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.EventsGenerators.categoryNames
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier, MicroserviceUrlFinder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class SubscriptionPayloadComposerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "prepareSubscriptionPayload" should {

    "return CategoryAndUrlPayload containing the given CategoryName and found subscriberUrl" in new TestCase {
      val microserviceUrl = microserviceBaseUrls.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(microserviceUrl.pure[Try])

      composer.prepareSubscriptionPayload() shouldBe json"""{
        "categoryName" : ${categoryName.value},
        "subscriber": {
          "url": ${(microserviceUrl / "events").value},
          "id":  ${microserviceId.value}
        }
      }""".pure[Try]
    }

    "fail if finding subscriberUrl fails" in new TestCase {
      val exception = exceptions.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(exception.raiseError[Try, MicroserviceBaseUrl])

      composer.prepareSubscriptionPayload() shouldBe exception.raiseError[Try, CategoryAndUrlPayload]
    }
  }

  private trait TestCase {
    val categoryName   = categoryNames.generateOne
    val microserviceId = MicroserviceIdentifier.generate
    val urlFinder      = mock[MicroserviceUrlFinder[Try]]
    val composer       = new SubscriptionPayloadComposerImpl[Try](categoryName, urlFinder, microserviceId)
  }
}
