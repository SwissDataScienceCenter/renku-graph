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

package io.renku.eventlog.events.producers

import Generators._
import SubscriptionCategory._
import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.Encoder
import io.renku.events.{CategoryName, Subscription}
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriptionCategorySpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

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

    implicit def encoder[S <: Subscription.Subscriber](implicit sEncoder: Encoder[S]): Encoder[(CategoryName, S)] =
      Encoder.instance { case (category, subscriber) =>
        json"""{
          "categoryName": $category
        }""" deepMerge subscriber.asJson
      }

    "return Accepted if payload decoding and registration succeeds" in new TestCase {

      val subscriber = testSubscribers.generateOne
      givenRegistration(of = subscriber, returning = ().pure[IO])

      subscriptionCategory.register((categoryName -> subscriber).asJson).unsafeRunSync() shouldBe AcceptedRegistration
    }

    "return Rejected if payload does not contain the same category name" in new TestCase {
      subscriptionCategory
        .register((categoryNames.generateOne -> testSubscribers.generateOne).asJson)
        .unsafeRunSync() shouldBe RejectedRegistration
    }

    "return Rejected if payload cannot be decoded" in new TestCase {
      subscriptionCategory.register(jsons.generateOne).unsafeRunSync() shouldBe RejectedRegistration
    }

    "fail if adding the subscriber url fails" in new TestCase {

      val subscriber = testSubscribers.generateOne

      val exception = exceptions.generateOne
      (subscribers.add _)
        .expects(subscriber)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscriptionCategory.register((categoryName -> subscriber).asJson).unsafeRunSync()
      } shouldBe exception
    }
  }

  "getStatus" should {

    "return info about category with capacity when exists" in new TestCase {

      val totalCapacity = totalCapacities.generateOne
      (() => subscribers.getTotalCapacity).expects().returning(totalCapacity.some)

      val usedCapacity = usedCapacities.generateOne
      (() => capacityFinder.findUsedCapacity).expects().returning(usedCapacity.pure[IO])

      subscriptionCategory.getStatus.unsafeRunSync() shouldBe EventProducerStatus(
        categoryName,
        EventProducerStatus.Capacity(totalCapacity, totalCapacity - usedCapacity).some
      )
    }

    "return info about category without capacity when does not exist" in new TestCase {

      (() => subscribers.getTotalCapacity).expects().returning(None)

      subscriptionCategory.getStatus.unsafeRunSync() shouldBe EventProducerStatus(
        categoryName,
        maybeCapacity = None
      )
    }
  }

  private trait TestCase {
    val categoryName      = categoryNames.generateOne
    val subscribers       = mock[Subscribers[IO, TestSubscriber]]
    val eventsDistributor = mock[EventsDistributor[IO]]
    val capacityFinder    = mock[CapacityFinder[IO]]
    val subscriptionCategory = new SubscriptionCategoryImpl[IO, TestSubscriber](
      categoryName,
      subscribers,
      eventsDistributor,
      capacityFinder
    )

    def givenRegistration(of: TestSubscriber, returning: IO[Unit]) =
      (subscribers.add _)
        .expects(of)
        .returning(returning)
  }
}
