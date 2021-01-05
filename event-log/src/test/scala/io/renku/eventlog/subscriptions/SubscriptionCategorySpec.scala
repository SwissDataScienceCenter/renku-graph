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

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeErrorId, catsSyntaxApplicativeId}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import io.renku.eventlog.subscriptions.Generators._
import io.renku.eventlog.subscriptions.SubscriptionCategory.{AcceptedRegistration, RejectedRegistration}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionCategorySpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {
    "return unit when event distributor run succeeds" in new TestCase {
      (eventsDistributor.run _)
        .expects()
        .returning(().pure[IO])

      subscriptionCategory.run().unsafeRunSync() shouldBe ()
    }

    "fail when event distributor returns an error" in new TestCase {
      val exception = exceptions.generateOne
      (eventsDistributor.run _)
        .expects()
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscriptionCategory.run().unsafeRunSync()
      } shouldBe exception

    }
  }

  "register" should {
    "return the subscriber URL if the statuses are valid" in new TestCase {
      val subscriptionCategoryPayload = subscriptionCategoryPayloads.generateOne
      val payload                     = jsons.generateOne
      (deserializer.deserialize _)
        .expects(payload)
        .returning(subscriptionCategoryPayload.some.pure[IO])

      (subscribers.add _)
        .expects(subscriptionCategoryPayload.subscriberUrl)
        .returning(().pure[IO])

      subscriptionCategory.register(payload).unsafeRunSync() shouldBe AcceptedRegistration
    }

    "return None if the payload does not contain the right supported statuses" in new TestCase {
      val payload = jsons.generateOne
      (deserializer.deserialize _)
        .expects(payload)
        .returning(none.pure[IO])

      subscriptionCategory.register(payload).unsafeRunSync() shouldBe RejectedRegistration
    }

    "fail if adding the subscriber url fails" in new TestCase {
      val subscriptionCategoryPayload = subscriptionCategoryPayloads.generateOne
      val exception                   = exceptions.generateOne
      val payload                     = jsons.generateOne

      (deserializer.deserialize _)
        .expects(payload)
        .returning(subscriptionCategoryPayload.some.pure[IO])

      (subscribers.add _)
        .expects(subscriptionCategoryPayload.subscriberUrl)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscriptionCategory.register(payload).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val eventsDistributor = mock[EventsDistributor[IO]]
    val subscribers       = mock[Subscribers[IO]]

    trait Deserializer extends SubscriptionRequestDeserializer[IO] {
      override type PayloadType = SubscriptionCategoryPayload
    }
    val deserializer = mock[Deserializer]

    val subscriptionCategory =
      new SubscriptionCategoryImpl[IO, SubscriptionCategoryPayload](subscribers, eventsDistributor, deserializer)
  }
}
