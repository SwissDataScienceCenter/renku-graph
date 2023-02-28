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

import cats.effect.IO
import cats.syntax.all._
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.{CategoryName, Subscription}
import io.renku.events.Generators._
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}
import io.renku.generators.Generators.exceptions
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

class SubscriptionMechanismSpec extends AnyWordSpec with IOSpec with Eventually with should.Matchers {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(3, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  "notifyAvailability" should {

    "send subscription for events" in new TestCase {

      val payload = payloads.generateOne
      payloadComposer.`expected prepareSubscriptionPayload responses`.add(payload.pure[IO])

      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> IO.unit)

      subscriber.renewSubscription().unsafeRunSync() shouldBe ()
    }

    "fail if composing the subscription payload fails" in new TestCase {

      val exception = exceptions.generateOne
      payloadComposer.`expected prepareSubscriptionPayload responses`.add(
        exception.raiseError[IO, TestSubscriptionPayload]
      )

      intercept[Exception] {
        subscriber.renewSubscription().unsafeRunSync()
      } shouldBe exception
    }

    "fail if posting the subscription payload fails" in new TestCase {

      val payload = payloads.generateOne
      payloadComposer.`expected prepareSubscriptionPayload responses`.add(payload.pure[IO])

      val exception = exceptions.generateOne
      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> exception.raiseError[IO, Unit])

      intercept[Exception] {
        subscriber.renewSubscription().unsafeRunSync()
      } shouldBe exception
    }
  }

  "run" should {

    "send/resend subscription for events" in new TestCase {

      val payload = payloads.generateOne

      payloadComposer.`expected prepareSubscriptionPayload responses`.add(payload.pure[IO])

      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> IO.unit)

      subscriber.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Info(s"$categoryName: Subscribed for events with ${payload.subscriber.url}, id = ${payload.subscriber.id}")
        )
      }
    }

    "log an error and retry if composing subscription payload fails" in new TestCase {

      val exception = exceptions.generateOne
      payloadComposer.`expected prepareSubscriptionPayload responses`.add(
        exception.raiseError[IO, TestSubscriptionPayload]
      )
      val payload = payloads.generateOne
      payloadComposer.`expected prepareSubscriptionPayload responses`.add(payload.pure[IO])

      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> IO.unit)

      subscriber.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: Composing subscription payload failed", exception),
          Info(s"$categoryName: Subscribed for events with ${payload.subscriber.url}, id = ${payload.subscriber.id}")
        )
      }
    }

    "log an error and retry if sending subscription payload fails" in new TestCase {

      val payload = payloads.generateOne
      payloadComposer.`expected prepareSubscriptionPayload default response`.set(payload.pure[IO])

      val exception = exceptions.generateOne
      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> exception.raiseError[IO, Unit])
      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> exception.raiseError[IO, Unit])
      subscriptionSender.`expected postToEventLog responses`.add(payload.asJson -> IO.unit)

      subscriber.run().unsafeRunAndForget()

      eventually {
        logger.loggedOnly(
          Error(s"$categoryName: Subscribing for events failed", exception),
          Error(s"$categoryName: Subscribing for events failed", exception),
          Info(s"$categoryName: Subscribed for events with ${payload.subscriber.url}, id = ${payload.subscriber.id}")
        )
      }
    }
  }

  private trait TestCase {
    val categoryName = categoryNames.generateOne

    val payloadComposer = new SubscriptionPayloadComposer[IO, TestSubscriptionPayload] {
      val `expected prepareSubscriptionPayload responses` = new ConcurrentLinkedQueue[IO[TestSubscriptionPayload]]()
      val `expected prepareSubscriptionPayload default response` = new AtomicReference[IO[TestSubscriptionPayload]]()

      override def prepareSubscriptionPayload(): IO[TestSubscriptionPayload] =
        Option(`expected prepareSubscriptionPayload responses`.poll())
          .getOrElse(`expected prepareSubscriptionPayload default response`.get())
    }

    val subscriptionSender = new SubscriptionSender[IO] {
      val `expected postToEventLog responses` = new ConcurrentLinkedQueue[(Json, IO[Unit])]()

      override def postToEventLog(payload: Json): IO[Unit] =
        Option {
          val (expectedPayload, response) = `expected postToEventLog responses`.poll()
          if (payload == expectedPayload) response
          else fail(s"Expected $expectedPayload in the postToEventLog but got $payload")
        }
          .getOrElse(`expected postToEventLog responses`.asScala.last._2)
    }

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val subscriber = new SubscriptionMechanismImpl[IO, TestSubscriptionPayload](categoryName,
                                                                                payloadComposer,
                                                                                subscriptionSender,
                                                                                initialDelay = 5 millis,
                                                                                renewDelay = 500 millis
    )
  }

  private lazy val payloads: Gen[TestSubscriptionPayload] =
    (categoryNames, subscriberUrls, subscriberIds).mapN((category, url, id) =>
      TestSubscriptionPayload(category, TestSubscriber(url, id))
    )
}

private final case class TestSubscriber(url: SubscriberUrl, id: SubscriberId) extends Subscription.Subscriber
private final case class TestSubscriptionPayload(categoryName: CategoryName, subscriber: TestSubscriber)
    extends Subscription

private object TestSubscriptionPayload {
  implicit val encoder: Encoder[TestSubscriptionPayload] = Encoder.instance {
    case TestSubscriptionPayload(category, TestSubscriber(url, id)) =>
      json"""{
        "categoryName": $category,
        "subscriber":   {
          "url": $url,
          "id":  $id
        }
      }"""
  }
}
