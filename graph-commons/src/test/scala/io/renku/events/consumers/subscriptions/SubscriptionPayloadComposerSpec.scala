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

package io.renku.events.consumers.subscriptions

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.{DefaultSubscription, Subscription}
import io.renku.events.DefaultSubscription.DefaultSubscriber
import io.renku.events.Generators._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.exceptions
import io.renku.generators.Generators.Implicits._
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier, MicroserviceUrlFinder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.TryValues

import scala.util.Try

class SubscriptionPayloadComposerSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "prepareSubscriptionPayload" should {

    "return SubscriptionPayload containing the given CategoryName and found subscriberUrl" in new TestCase {

      val microserviceUrl = microserviceBaseUrls.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(microserviceUrl.pure[Try])

      val composer =
        new DefaultSubscriptionPayloadComposer[Try](categoryName, urlFinder, microserviceId, maybeCapacity = None)

      composer.prepareSubscriptionPayload().success.value shouldBe DefaultSubscription(
        categoryName,
        DefaultSubscriber(Subscription.SubscriberUrl(microserviceUrl, "events"),
                          Subscription.SubscriberId(microserviceId)
        )
      )
    }

    "return SubscriptionPayload containing the given CategoryName, found subscriberUrl and the given capacity" in new TestCase {

      val microserviceUrl = microserviceBaseUrls.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(microserviceUrl.pure[Try])

      val capacity = subscriberCapacities.generateOne

      val composer = new DefaultSubscriptionPayloadComposer[Try](categoryName,
                                                                 urlFinder,
                                                                 microserviceId,
                                                                 maybeCapacity = capacity.some
      )

      composer.prepareSubscriptionPayload().success.value shouldBe DefaultSubscription(
        categoryName,
        DefaultSubscriber(Subscription.SubscriberUrl(microserviceUrl, "events"),
                          Subscription.SubscriberId(microserviceId),
                          capacity
        )
      )
    }

    "fail if finding subscriberUrl fails" in new TestCase {

      val exception = exceptions.generateOne
      (urlFinder.findBaseUrl _)
        .expects()
        .returning(exception.raiseError[Try, MicroserviceBaseUrl])

      new DefaultSubscriptionPayloadComposer[Try](categoryName, urlFinder, microserviceId, maybeCapacity = None)
        .prepareSubscriptionPayload()
        .failure
        .exception shouldBe exception
    }
  }

  private trait TestCase {
    val categoryName   = categoryNames.generateOne
    val microserviceId = MicroserviceIdentifier.generate
    val urlFinder      = mock[MicroserviceUrlFinder[Try]]
  }
}
