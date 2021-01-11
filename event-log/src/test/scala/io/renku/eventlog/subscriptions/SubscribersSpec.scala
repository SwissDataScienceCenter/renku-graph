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

import cats.effect.concurrent.Deferred
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import io.renku.eventlog.subscriptions.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class SubscribersSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "add" should {

    "adds the given subscriber to the registry and logs info when it was added" in new TestCase {

      (subscribersRegistry.add _)
        .expects(subscriberUrl)
        .returning(true.pure[IO])

      subscribers.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info(s"$categoryName: $subscriberUrl added"))
    }

    "adds the given subscriber to the registry and do not log info message when it was already added" in new TestCase {

      (subscribersRegistry.add _)
        .expects(subscriberUrl)
        .returning(false.pure[IO])

      subscribers.add(subscriberUrl).unsafeRunSync() shouldBe ((): Unit)

      logger.expectNoLogs()
    }
  }

  "runOnSubscriber" should {

    "run the given function on subscriber once available" in new TestCase {

      val subscriberUrlReference = mock[Deferred[IO, SubscriberUrl]]

      (() => subscriberUrlReference.get)
        .expects()
        .returning(subscriberUrl.pure[IO])

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(subscriberUrlReference.pure[IO])

      val function = mockFunction[SubscriberUrl, IO[Unit]]
      function.expects(subscriberUrl).returning(IO.unit)

      subscribers.runOnSubscriber(function).unsafeRunSync() shouldBe ((): Unit)
    }

    "fail if the given function fails when run on available subscriber" in new TestCase {

      val subscriberUrlReference = mock[Deferred[IO, SubscriberUrl]]

      (() => subscriberUrlReference.get)
        .expects()
        .returning(subscriberUrl.pure[IO])

      (subscribersRegistry.findAvailableSubscriber _)
        .expects()
        .returning(subscriberUrlReference.pure[IO])

      val exception = exceptions.generateOne
      val function  = mockFunction[SubscriberUrl, IO[Unit]]
      function.expects(subscriberUrl).returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscribers.runOnSubscriber(function).unsafeRunSync()
      } shouldBe exception
    }
  }

  "delete" should {

    "completely remove a subscriber from the registry" in new TestCase {
      (subscribersRegistry.delete _)
        .expects(subscriberUrl)
        .returning(true.pure[IO])

      subscribers.delete(subscriberUrl = subscriberUrl).unsafeRunSync()

      logger.loggedOnly(Info(s"$categoryName: $subscriberUrl gone - deleting"))
    }

    "not log if nothing was deleted" in new TestCase {
      (subscribersRegistry.delete _)
        .expects(subscriberUrl)
        .returning(false.pure[IO])

      subscribers.delete(subscriberUrl = subscriberUrl).unsafeRunSync()

      logger.expectNoLogs()
    }
  }

  "markBusy" should {
    "put on hold selected subscriber" in new TestCase {
      (subscribersRegistry.markBusy _)
        .expects(subscriberUrl)
        .returning(IO.unit)

      subscribers.markBusy(subscriberUrl).unsafeRunSync()

      logger.expectNoLogs()
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val categoryName  = categoryNames.generateOne
    val subscriberUrl = subscriberUrls.generateOne

    val subscribersRegistry = mock[SubscribersRegistry]
    val logger              = TestLogger[IO]()
    val subscribers         = new SubscribersImpl(categoryName, subscribersRegistry, logger)
  }
}
