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

package io.renku.eventlog.subscriptions.zombieevents

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.events.consumers.subscriptions._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.ServiceHealthChecker
import io.renku.metrics.TestLabeledHistogram
import io.renku.microservices.MicroserviceBaseUrl
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._

class ZombieNodesCleanerSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "removeZombieNodes" should {

    "do nothing if there are no rows in the subscriber table" in new TestCase {

      cleaner.removeZombieNodes().unsafeRunSync() shouldBe ()

      findAllSubscribers() shouldBe List()
    }

    "do nothing if all sources and subscribers listed in the subscriber table are up" in new TestCase {

      val subscriber1Id  = subscriberIds.generateOne
      val subscriber1Url = subscriberUrls.generateOne
      (serviceHealthChecker.ping _)
        .expects(subscriber1Url.toUnsafe[MicroserviceBaseUrl])
        .returning(true.pure[IO])
        .atLeastOnce()
      upsertSubscriber(subscriber1Id, subscriber1Url, microserviceBaseUrl)

      val subscriber2Id  = subscriberIds.generateOne
      val subscriber2Url = subscriberUrls.generateOne
      (serviceHealthChecker.ping _)
        .expects(subscriber2Url.toUnsafe[MicroserviceBaseUrl])
        .returning(true.pure[IO])
        .atLeastOnce()
      upsertSubscriber(subscriber2Id, subscriber2Url, microserviceBaseUrl)

      val otherSources = microserviceBaseUrls.generateNonEmptyList()
      otherSources.map(upsertSubscriber(subscriber1Id, subscriber1Url, _))

      otherSources.map {
        (serviceHealthChecker.ping _).expects(_).returning(true.pure[IO])
      }

      cleaner.removeZombieNodes().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs (otherSources :+ microserviceBaseUrl)
        .map(sourceUrl => (subscriber1Id, subscriber1Url, sourceUrl))
        .toList :+ (subscriber2Id, subscriber2Url, microserviceBaseUrl)
    }

    "remove rows from the subscriber table if both the sources and the deliveries are inactive " +
      "but move active subscribers on inactive source to the current source" in new TestCase {

        val activeSubscriberId  = subscriberIds.generateOne
        val activeSubscriberUrl = subscriberUrls.generateOne
        (serviceHealthChecker.ping _)
          .expects(activeSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
          .returning(true.pure[IO])
          .atLeastOnce()
        upsertSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)

        val anotherActiveSubscriberId  = subscriberIds.generateOne
        val anotherActiveSubscriberUrl = subscriberUrls.generateOne
        (serviceHealthChecker.ping _)
          .expects(anotherActiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
          .returning(true.pure[IO])
          .atLeastOnce()

        val inactiveSubscriberId  = subscriberIds.generateOne
        val inactiveSubscriberUrl = subscriberUrls.generateOne
        (serviceHealthChecker.ping _)
          .expects(inactiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
          .returning(false.pure[IO])
          .atLeastOnce()
        upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, microserviceBaseUrl)

        val otherSources = microserviceBaseUrls.generateNonEmptyList(minElements = 3)
        otherSources map (upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, _))

        val someInactiveSource = otherSources.head
        upsertSubscriber(anotherActiveSubscriberId, anotherActiveSubscriberUrl, someInactiveSource)
        upsertSubscriber(activeSubscriberId, activeSubscriberUrl, someInactiveSource)

        val activeSource = otherSources.toList(otherSources.size / 2)
        otherSources map {
          case `activeSource` =>
            (serviceHealthChecker.ping _).expects(activeSource).returning(true.pure[IO]).atLeastOnce()
          case inactive =>
            (serviceHealthChecker.ping _).expects(inactive).returning(false.pure[IO]).atLeastOnce()
        }
        upsertSubscriber(activeSubscriberId, activeSubscriberUrl, activeSource)

        cleaner.removeZombieNodes().unsafeRunSync() shouldBe ()

        findAllSubscribers() should contain theSameElementsAs List(
          (activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl),
          (activeSubscriberId, activeSubscriberUrl, activeSource),
          (anotherActiveSubscriberId, anotherActiveSubscriberUrl, microserviceBaseUrl)
        )
      }

    "remove rows from the subscriber table if there are inactive subscriber on the current source" in new TestCase {

      val activeSubscriberId  = subscriberIds.generateOne
      val activeSubscriberUrl = subscriberUrls.generateOne
      (serviceHealthChecker.ping _)
        .expects(activeSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
        .returning(true.pure[IO])
        .atLeastOnce()
      upsertSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)

      val inactiveSubscriberId  = subscriberIds.generateOne
      val inactiveSubscriberUrl = subscriberUrls.generateOne
      (serviceHealthChecker.ping _)
        .expects(inactiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
        .returning(false.pure[IO])
        .atLeastOnce()
      upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, microserviceBaseUrl)

      cleaner.removeZombieNodes().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs List(
        (activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)
      )
    }
  }

  private trait TestCase {
    val queriesExecTimes     = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val microserviceBaseUrl  = microserviceBaseUrls.generateOne
    val serviceHealthChecker = mock[ServiceHealthChecker[IO]]
    val cleaner =
      new ZombieNodesCleanerImpl(sessionResource, queriesExecTimes, microserviceBaseUrl, serviceHealthChecker)
  }

  private def findAllSubscribers(): List[(SubscriberId, SubscriberUrl, MicroserviceBaseUrl)] = execute {
    Kleisli { session =>
      val query: Query[Void, (SubscriberId, SubscriberUrl, MicroserviceBaseUrl)] = sql"""
          SELECT DISTINCT delivery_id, delivery_url, source_url
          FROM subscriber
          """
        .query(subscriberIdDecoder ~ subscriberUrlDecoder ~ microserviceBaseUrlDecoder)
        .map { case subscriberId ~ subscriberUrl ~ microserviceBaseUrl =>
          (subscriberId, subscriberUrl, microserviceBaseUrl)
        }
      session.execute(query)
    }
  }
}
