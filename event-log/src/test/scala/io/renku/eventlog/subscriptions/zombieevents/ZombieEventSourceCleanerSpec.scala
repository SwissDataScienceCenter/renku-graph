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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl, _}
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.metrics.TestLabeledHistogram
import ch.datascience.microservices.MicroserviceBaseUrl
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ZombieEventSourceCleanerSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with MockFactory
    with should.Matchers {

  "removeZombieSources" should {

    "do nothing if there are no rows in the subscriber table" in new TestCase {

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() shouldBe List()
    }

    "do nothing if there are not other sources in the subscriber table" in new TestCase {

      val subscriberId  = subscriberIds.generateOne
      val subscriberUrl = subscriberUrls.generateOne
      upsertSubscriber(subscriberId, subscriberUrl, microserviceBaseUrl)

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() shouldBe List(
        (subscriberId, subscriberUrl, microserviceBaseUrl)
      )
    }

    "do nothing if there are other sources in the subscriber table but they are active" in new TestCase {

      val subscriberId  = subscriberIds.generateOne
      val subscriberUrl = subscriberUrls.generateOne
      (serviceHealthChecker.ping _)
        .expects(subscriberUrl.toUnsafe[MicroserviceBaseUrl])
        .returning(true.pure[IO])
        .atLeastOnce()

      val otherSources = microserviceBaseUrls.generateNonEmptyList()
      upsertSubscriber(subscriberId, subscriberUrl, microserviceBaseUrl)

      otherSources.map(upsertSubscriber(subscriberId, subscriberUrl, _))

      otherSources.map {
        (serviceHealthChecker.ping _).expects(_).returning(true.pure[IO])
      }

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs (otherSources :+ microserviceBaseUrl)
        .map(sourceUrl => (subscriberId, subscriberUrl, sourceUrl))
        .toList
    }

    "remove rows from the subscriber table if both the sources and the deliveries are inactive" in new TestCase {

      val activeSubscriberId    = subscriberIds.generateOne
      val activeSubscriberUrl   = subscriberUrls.generateOne
      val inactiveSubscriberId  = subscriberIds.generateOne
      val inactiveSubscriberUrl = subscriberUrls.generateOne
      (serviceHealthChecker.ping _)
        .expects(activeSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
        .returning(true.pure[IO])
        .atLeastOnce()
      (serviceHealthChecker.ping _)
        .expects(inactiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl])
        .returning(false.pure[IO])
        .atLeastOnce()

      val otherSources = microserviceBaseUrls.generateNonEmptyList(minElements = 3)
      upsertSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)
      upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, microserviceBaseUrl)

      otherSources.map(upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, _))

      val someInactiveSource = otherSources.head
      upsertSubscriber(activeSubscriberId, activeSubscriberUrl, someInactiveSource)

      val activeSource = otherSources.toList(otherSources.size / 2)
      otherSources map {
        case `activeSource` =>
          (serviceHealthChecker.ping _).expects(activeSource).returning(true.pure[IO]).atLeastOnce()
        case inactive =>
          (serviceHealthChecker.ping _).expects(inactive).returning(false.pure[IO]).atLeastOnce()
      }

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs List(
        (activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl),
        (inactiveSubscriberId, inactiveSubscriberUrl, microserviceBaseUrl),
        (inactiveSubscriberId, inactiveSubscriberUrl, activeSource),
        (activeSubscriberId, activeSubscriberUrl, someInactiveSource)
      )
    }

  }

  private trait TestCase {
    val queriesExecTimes     = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val microserviceBaseUrl  = microserviceBaseUrls.generateOne
    val serviceHealthChecker = mock[ServiceHealthChecker[IO]]
    val cleaner =
      new ZombieEventSourceCleanerImpl(transactor, queriesExecTimes, microserviceBaseUrl, serviceHealthChecker)
  }

  private def findAllSubscribers(): List[(SubscriberId, SubscriberUrl, MicroserviceBaseUrl)] = execute {
    sql"""|SELECT DISTINCT delivery_id, delivery_url, source_url
          |FROM subscriber
          |""".stripMargin
      .query[(SubscriberId, SubscriberUrl, MicroserviceBaseUrl)]
      .to[List]
  }
}
