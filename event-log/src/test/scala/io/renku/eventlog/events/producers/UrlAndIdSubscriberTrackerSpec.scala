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
import cats.data.Kleisli
import cats.effect.IO
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.subscriptions._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.metrics.TestMetricsRegistry
import io.renku.microservices.MicroserviceBaseUrl
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

class UrlAndIdSubscriberTrackerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "add" should {

    "insert a new row in the subscriber table if the subscriber does not exists" in new TestCase {

      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe None

      (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
      )
    }

    "replace existing row for the subscription info with the same subscriber url but different subscriber id" in new TestCase {

      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe None

      (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
      )

      val newSubscriberId = subscriberIds.generateOne
      (tracker add subscriptionInfo.copy(subscriberId = newSubscriberId)).unsafeRunSync() shouldBe true

      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
        (newSubscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
      )
    }

    "insert a new row in the subscriber table " +
      "if the subscriber exists but the source_url is different" in new TestCase {
        val otherSource  = microserviceBaseUrls.generateOne
        val otherTracker = new UrlAndIdSubscriberTrackerImpl[IO](otherSource)
        (otherTracker add subscriptionInfo).unsafeRunSync() shouldBe true

        findSubscriber(subscriptionInfo.subscriberUrl, otherSource) shouldBe Some(
          (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, otherSource)
        )
        findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe None

        (tracker add subscriptionInfo).unsafeRunSync() shouldBe true

        findSubscriber(subscriptionInfo.subscriberUrl, otherSource) shouldBe Some(
          (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, otherSource)
        )
        findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
          (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
        )
      }

    "do nothing if the subscriber info is already present in the table" in new TestCase {
      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe None

      (tracker add subscriptionInfo).unsafeRunSync() shouldBe true
      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
      )

      (tracker add subscriptionInfo).unsafeRunSync() shouldBe true
      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
      )
    }
  }

  "remove" should {

    "remove a subscriber if the subscriber and the current source url exists" in new TestCase {
      storeSubscriptionInfo(subscriptionInfo, sourceUrl)

      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, sourceUrl)
      )
      (tracker remove subscriptionInfo.subscriberUrl).unsafeRunSync() shouldBe true
      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl)       shouldBe None

    }

    "do nothing if the subscriber does not exists" in new TestCase {
      (tracker remove subscriptionInfo.subscriberUrl).unsafeRunSync() shouldBe true
      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl)       shouldBe None
    }

    "do nothing if the subscriber exists but the source_url is different than the current source url" in new TestCase {

      val otherSource = microserviceBaseUrls.generateOne
      storeSubscriptionInfo(subscriptionInfo, otherSource)
      findSubscriber(subscriptionInfo.subscriberUrl, otherSource) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, otherSource)
      )

      (tracker remove subscriptionInfo.subscriberUrl).unsafeRunSync() shouldBe true
      findSubscriber(subscriptionInfo.subscriberUrl, otherSource) shouldBe Some(
        (subscriptionInfo.subscriberId, subscriptionInfo.subscriberUrl, otherSource)
      )
      findSubscriber(subscriptionInfo.subscriberUrl, sourceUrl) shouldBe None
    }
  }

  private trait TestCase {
    val subscriptionInfo = urlAndIdSubscriptionInfos.generateOne
    private implicit val metricsRegistry: TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    implicit val queriesExecTimes:        QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val sourceUrl = microserviceBaseUrls.generateOne
    val tracker   = new UrlAndIdSubscriberTrackerImpl[IO](sourceUrl)
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

  private def storeSubscriptionInfo(subscriptionInfo: SubscriptionInfo, sourceUrl: MicroserviceBaseUrl): Unit =
    execute[Unit] {
      Kleisli { session =>
        val query: Command[SubscriberId ~ SubscriberUrl ~ MicroserviceBaseUrl] =
          sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
                VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
          """.command
        session
          .prepare(query)
          .flatMap(_.execute(subscriptionInfo.subscriberId ~ subscriptionInfo.subscriberUrl ~ sourceUrl))
          .void
      }
    }
}
