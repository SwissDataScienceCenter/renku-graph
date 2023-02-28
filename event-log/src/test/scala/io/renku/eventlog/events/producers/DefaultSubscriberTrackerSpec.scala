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

import cats.data.Kleisli
import cats.effect.IO
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.Generators._
import io.renku.events.Subscription
import io.renku.events.Subscription._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.metrics.TestMetricsRegistry
import io.renku.microservices.MicroserviceBaseUrl
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues
import skunk._
import skunk.implicits._

class DefaultSubscriberTrackerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with OptionValues
    with should.Matchers {

  "add" should {

    "insert a new row in the subscriber table if the subscriber does not exists" in new TestCase {

      findSubscriber(subscriber.url, sourceUrl) shouldBe None

      (tracker add subscriber).unsafeRunSync() shouldBe true

      findSubscriber(subscriber.url, sourceUrl).value shouldBe (subscriber.id, subscriber.url, sourceUrl)
    }

    "replace existing row for the subscription info with the same subscriber url but different subscriber id" in new TestCase {

      findSubscriber(subscriber.url, sourceUrl) shouldBe None

      (tracker add subscriber).unsafeRunSync() shouldBe true

      findSubscriber(subscriber.url, sourceUrl).value shouldBe (subscriber.id, subscriber.url, sourceUrl)

      val newSubscriberId = subscriberIds.generateOne
      (tracker add subscriber.fold(_.copy(id = newSubscriberId), _.copy(id = newSubscriberId)))
        .unsafeRunSync() shouldBe true

      findSubscriber(subscriber.url, sourceUrl).value shouldBe (newSubscriberId, subscriber.url, sourceUrl)
    }

    "insert a new row in the subscriber table " +
      "if the subscriber exists but the source_url is different" in new TestCase {

        val otherSource  = microserviceBaseUrls.generateOne
        val otherTracker = new DefaultSubscriberTrackerImpl[IO](otherSource)
        (otherTracker add subscriber).unsafeRunSync() shouldBe true

        findSubscriber(subscriber.url, otherSource).value shouldBe (subscriber.id, subscriber.url, otherSource)

        findSubscriber(subscriber.url, sourceUrl) shouldBe None

        (tracker add subscriber).unsafeRunSync() shouldBe true

        findSubscriber(subscriber.url, otherSource).value shouldBe (subscriber.id, subscriber.url, otherSource)
        findSubscriber(subscriber.url, sourceUrl).value   shouldBe (subscriber.id, subscriber.url, sourceUrl)
      }

    "do nothing if the subscriber info is already present in the table" in new TestCase {

      findSubscriber(subscriber.url, sourceUrl) shouldBe None

      (tracker add subscriber).unsafeRunSync()        shouldBe true
      findSubscriber(subscriber.url, sourceUrl).value shouldBe (subscriber.id, subscriber.url, sourceUrl)

      (tracker add subscriber).unsafeRunSync()        shouldBe true
      findSubscriber(subscriber.url, sourceUrl).value shouldBe (subscriber.id, subscriber.url, sourceUrl)
    }
  }

  "remove" should {

    "remove a subscriber if the subscriber and the current source url exists" in new TestCase {

      storeSubscriptionInfo(subscriber, sourceUrl)

      findSubscriber(subscriber.url, sourceUrl).value shouldBe (subscriber.id, subscriber.url, sourceUrl)

      (tracker remove subscriber.url).unsafeRunSync() shouldBe true
      findSubscriber(subscriber.url, sourceUrl)       shouldBe None
    }

    "do nothing if the subscriber does not exists" in new TestCase {
      (tracker remove subscriber.url).unsafeRunSync() shouldBe true
      findSubscriber(subscriber.url, sourceUrl)       shouldBe None
    }

    "do nothing if the subscriber exists but the source_url is different than the current source url" in new TestCase {

      val otherSource = microserviceBaseUrls.generateOne
      storeSubscriptionInfo(subscriber, otherSource)
      findSubscriber(subscriber.url, otherSource).value shouldBe (subscriber.id, subscriber.url, otherSource)

      (tracker remove subscriber.url).unsafeRunSync()   shouldBe true
      findSubscriber(subscriber.url, otherSource).value shouldBe (subscriber.id, subscriber.url, otherSource)

      findSubscriber(subscriber.url, sourceUrl) shouldBe None
    }
  }

  private trait TestCase {

    val subscriber = subscribers.generateOne

    private implicit val metricsRegistry: TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    implicit val queriesExecTimes:        QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val sourceUrl = microserviceBaseUrls.generateOne
    val tracker   = new DefaultSubscriberTrackerImpl[IO](sourceUrl)
  }

  private def findSubscriber(subscriberUrl: SubscriberUrl,
                             sourceUrl:     MicroserviceBaseUrl
  ): Option[(SubscriberId, SubscriberUrl, MicroserviceBaseUrl)] = execute {
    Kleisli { session =>
      val query: Query[SubscriberUrl ~ MicroserviceBaseUrl, (SubscriberId, SubscriberUrl, MicroserviceBaseUrl)] =
        sql"""SELECT delivery_id, delivery_url, source_url
              FROM subscriber
              WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder"""
          .query(subscriberIdDecoder ~ subscriberUrlDecoder ~ microserviceBaseUrlDecoder)
          .map { case subscriberId ~ subscriberUrl ~ microserviceBaseUrl =>
            (subscriberId, subscriberUrl, microserviceBaseUrl)
          }
      session.prepare(query).flatMap(_.option(subscriberUrl ~ sourceUrl))
    }
  }

  private def storeSubscriptionInfo(subscriber: Subscription.Subscriber, sourceUrl: MicroserviceBaseUrl): Unit =
    execute[Unit] {
      Kleisli { session =>
        val query: Command[SubscriberId ~ SubscriberUrl ~ MicroserviceBaseUrl] =
          sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
                VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
          """.command
        session
          .prepare(query)
          .flatMap(_.execute(subscriber.id ~ subscriber.url ~ sourceUrl))
          .void
      }
    }
}
