/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.{Deferred, IO}
import cats.syntax.all._
import io.renku.events.Generators.categoryNames
import io.renku.events.Subscription.SubscriberUrl
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscribersSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "add" should {

    "adds the given subscriber to the registry and logs info when it was added" in new TestCase {

      (subscribersRegistry.add _)
        .expects(subscriber)
        .returning(true.pure[IO])

      (subscriberTracker.add _).expects(subscriber).returning(true.pure[IO])

      subscribers.add(subscriber).unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info(show"$categoryName: $subscriber added"))
    }

    "adds the given subscriber to the registry and do not log info message when it was already added" in new TestCase {

      (subscribersRegistry.add _)
        .expects(subscriber)
        .returning(false.pure[IO])

      (subscriberTracker.add _).expects(subscriber).returning(false.pure[IO])

      subscribers.add(subscriber).unsafeRunSync() shouldBe ()

      logger.expectNoLogs()
    }
  }

  "runOnSubscriber" should {

    "run the given function on subscriber once available" in new TestCase {

      val subscriberUrlReference = mock[Deferred[IO, SubscriberUrl]]

      (() => subscriberUrlReference.get)
        .expects()
        .returning(subscriber.url.pure[IO])

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(subscriberUrlReference.pure[IO])

      val function = mockFunction[SubscriberUrl, IO[Unit]]
      function.expects(subscriber.url).returning(IO.unit)

      subscribers.runOnSubscriber(function).unsafeRunSync() shouldBe ()
    }

    "fail if the given function fails when run on available subscriber" in new TestCase {

      val subscriberUrlReference = mock[Deferred[IO, SubscriberUrl]]

      (() => subscriberUrlReference.get)
        .expects()
        .returning(subscriber.url.pure[IO])

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(subscriberUrlReference.pure[IO])

      val exception = exceptions.generateOne
      val function  = mockFunction[SubscriberUrl, IO[Unit]]
      function.expects(subscriber.url).returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscribers.runOnSubscriber(function).unsafeRunSync()
      } shouldBe exception
    }
  }

  "delete" should {

    "completely remove a subscriber from the registry" in new TestCase {
      (subscribersRegistry.delete _)
        .expects(subscriber.url)
        .returning(true.pure[IO])

      (subscriberTracker.remove _).expects(subscriber.url).returning(true.pure[IO])

      subscribers.delete(subscriber.url).unsafeRunSync()

      logger.loggedOnly(Info(show"$categoryName: ${subscriber.url} gone - deleting"))
    }

    "not log if nothing was deleted" in new TestCase {
      (subscribersRegistry.delete _)
        .expects(subscriber.url)
        .returning(false.pure[IO])

      (subscriberTracker.remove _).expects(subscriber.url).returning(false.pure[IO])

      subscribers.delete(subscriber.url).unsafeRunSync()

      logger.expectNoLogs()
    }
  }

  "markBusy" should {
    "put on hold selected subscriber" in new TestCase {
      (subscribersRegistry.markBusy _)
        .expects(subscriber.url)
        .returning(IO.unit)

      subscribers.markBusy(subscriber.url).unsafeRunSync()

      logger.expectNoLogs()
    }
  }

  "getTotalCapacity" should {

    "return totalCapacity fetched from the registry" in new TestCase {
      val maybeCapacity = totalCapacities.generateOption
      (() => subscribersRegistry.getTotalCapacity)
        .expects()
        .returning(maybeCapacity)

      subscribers.getTotalCapacity shouldBe maybeCapacity
    }
  }

  private trait TestCase {
    val categoryName = categoryNames.generateOne
    val subscriber   = testSubscribers.generateOne

    val subscribersRegistry = mock[SubscribersRegistry[IO]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    implicit val subscriberTracker: SubscriberTracker[IO, TestSubscriber] =
      mock[SubscriberTracker[IO, TestSubscriber]]
    val subscribers = new SubscribersImpl(categoryName, subscribersRegistry)
  }
}
