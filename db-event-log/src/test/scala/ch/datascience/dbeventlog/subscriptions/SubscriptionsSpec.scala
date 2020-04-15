/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.subscriptions

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class SubscriptionsSpec extends WordSpec {

  "add and getAll" should {

    "add the given Subscription URL to the pool " +
      "if it's not present there yet" in new TestCase {
      subscriptions.add(subscriptionUrl) shouldBe ().pure[Try]
      subscriptions.getAll               shouldBe List(subscriptionUrl).pure[Try]

      val otherSubscriptionUrl = subscriptionUrls.generateOne
      subscriptions.add(otherSubscriptionUrl) shouldBe ().pure[Try]
      subscriptions.getAll.get                should contain theSameElementsAs List(subscriptionUrl, otherSubscriptionUrl)

      logger.loggedOnly(
        Info(s"$subscriptionUrl added"),
        Info(s"$otherSubscriptionUrl added")
      )
    }

    "do nothing if the given Subscription URL is already present in the pool" in new TestCase {
      subscriptions.add(subscriptionUrl) shouldBe ().pure[Try]
      subscriptions.getAll               shouldBe List(subscriptionUrl).pure[Try]

      subscriptions.add(subscriptionUrl) shouldBe ().pure[Try]
      subscriptions.getAll               shouldBe List(subscriptionUrl).pure[Try]

      logger.loggedOnly(Info(s"$subscriptionUrl added"))
    }
  }

  "remove" should {

    "remove the given Subscription URL if present in the pool" in new TestCase {
      subscriptions.add(subscriptionUrl) shouldBe ().pure[Try]
      subscriptions.getAll               shouldBe List(subscriptionUrl).pure[Try]

      subscriptions.remove(subscriptionUrl) shouldBe ().pure[Try]

      logger.loggedOnly(
        Info(s"$subscriptionUrl added"),
        Info(s"$subscriptionUrl removed")
      )
    }

    "do nothing when given Subscription URL does not exist in the pool" in new TestCase {
      subscriptions.remove(subscriptionUrl) shouldBe ().pure[Try]
    }
  }

  private trait TestCase {
    val subscriptionUrl = subscriptionUrls.generateOne

    val logger        = TestLogger[Try]()
    val subscriptions = new Subscriptions[Try](logger)
  }
}
