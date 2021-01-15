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

package ch.datascience.triplesgenerator.events.subscriptions

import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.EventsGenerators.categoryNames
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class SubscriptionMechanismSpec extends AnyWordSpec with MockFactory with Eventually with should.Matchers {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(3, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "notifyAvailability" should {

    "send subscription for events" in new TestCase {
      val subscriberUrl = subscriberUrls.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(subscriberUrl.pure[IO])

      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(IO.unit)

      subscriber.renewSubscription().unsafeRunSync() shouldBe ((): Unit)
    }

    "fail if finding the subscriber url fails" in new TestCase {
      val exception = exceptions.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(exception.raiseError[IO, SubscriberUrl])

      intercept[Exception] {
        subscriber.renewSubscription().unsafeRunSync()
      } shouldBe exception
    }

    "fail if posting the subscriber url fails" in new TestCase {
      val subscriberUrl = subscriberUrls.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(subscriberUrl.pure[IO])

      val exception = exceptions.generateOne
      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscriber.renewSubscription().unsafeRunSync()
      } shouldBe exception
    }
  }

  "run" should {

    "send/resend subscription for events" in new TestCase {

      val subscriberUrl = subscriberUrls.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(subscriberUrl.pure[IO])
        .atLeastOnce()

      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(IO.unit)
        .atLeastOnce()

      sleep(500)

      subscriber.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: Subscribed for events with $subscriberUrl")
        )
      }
    }

    "log an error and retry if finding Subscriber URL fails" in new TestCase {

      val exception = exceptions.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(exception.raiseError[IO, SubscriberUrl])

      val subscriberUrl = subscriberUrls.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(subscriberUrl.pure[IO])
        .atLeastOnce()

      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(IO.unit)
        .atLeastOnce()

      subscriber.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: Finding subscriber URL failed", exception),
          Info(s"$categoryName: Subscribed for events with $subscriberUrl")
        )
      }
    }

    "log an error and retry if sending Subscription URL fails" in new TestCase {

      val subscriberUrl = subscriberUrls.generateOne
      (urlFinder.findSubscriberUrl _)
        .expects()
        .returning(subscriberUrl.pure[IO])
        .atLeastOnce()

      val exception = exceptions.generateOne
      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(exception.raiseError[IO, Unit])
      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(exception.raiseError[IO, Unit])
      (subscriptionSender.postToEventLog _)
        .expects(subscriberUrl)
        .returning(IO.unit)
        .atLeastOnce()

      sleep(500)

      subscriber.run().unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: Subscribing for events failed", exception),
          Error(s"$categoryName: Subscribing for events failed", exception),
          Info(s"$categoryName: Subscribed for events with $subscriberUrl")
        )
      }
    }
  }

  private implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  private trait TestCase {
    val categoryName       = categoryNames.generateOne
    val urlFinder          = mock[SubscriptionUrlFinder[IO]]
    val subscriptionSender = mock[SubscriptionSender[IO]]
    val logger             = TestLogger[IO]()
    val subscriber =
      new SubscriptionMechanismImpl(categoryName,
                                    urlFinder,
                                    subscriptionSender,
                                    logger,
                                    initialDelay = 5 millis,
                                    renewDelay = 500 millis
      )
  }
}
