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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration.subscriptions

import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.events.Generators.categoryNames
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, positiveInts}
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier, MicroserviceUrlFinder}
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.GenerationProcessesNumber
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class PayloadComposerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "prepareSubscriptionPayload" should {

    "return Payload containing the given CategoryName, found subscriberUrl and capacity" in new TestCase {
      val microserviceUrl = microserviceBaseUrls.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(microserviceUrl.pure[Try])

      composer.prepareSubscriptionPayload() shouldBe json"""{
        "categoryName" : ${categoryName.value},
        "subscriber": {
          "url":      ${(microserviceUrl / "events").value},
          "id":       ${microserviceId.value},
          "capacity": ${capacity.value}
        }
      }""".pure[Try]
    }

    "fail if finding subscriberUrl fails" in new TestCase {
      val exception = exceptions.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(exception.raiseError[Try, MicroserviceBaseUrl])

      composer.prepareSubscriptionPayload() shouldBe exception.raiseError[Try, Json]
    }
  }

  private trait TestCase {
    val categoryName   = categoryNames.generateOne
    val capacity       = positiveInts().map(v => GenerationProcessesNumber(v.value)).generateOne
    val urlFinder      = mock[MicroserviceUrlFinder[Try]]
    val microserviceId = MicroserviceIdentifier.generate
    val composer       = new PayloadComposer[Try](categoryName, capacity, urlFinder, microserviceId)
  }
}
