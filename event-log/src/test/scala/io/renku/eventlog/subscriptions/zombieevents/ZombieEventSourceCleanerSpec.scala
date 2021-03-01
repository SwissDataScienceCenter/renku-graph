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
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.metrics.TestLabeledHistogram
import ch.datascience.microservices.MicroserviceBaseUrl
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.Generators.subscriberUrls
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

      val subscriberUrl = subscriberUrls.generateOne
      upsertSubscriber(subscriberUrl, microserviceBaseUrl)
      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() shouldBe List(subscriberUrl -> microserviceBaseUrl)
    }

    "do nothing if there are other sources in the subscriber table but they are active" in new TestCase {

      val subscriberUrl = subscriberUrls.generateOne
      val otherSources  = microserviceBaseUrls.generateNonEmptyList()
      upsertSubscriber(subscriberUrl, microserviceBaseUrl)

      otherSources.map(upsertSubscriber(subscriberUrl, _))

      otherSources.map {
        (serviceHealthChecker.ping _).expects(_).returning(true.pure[IO])
      }

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs (otherSources :+ microserviceBaseUrl)
        .map(sourceUrl => subscriberUrl -> sourceUrl)
        .toList
    }

    "remove other sources from the subscriber table if they are inactive" in new TestCase {

      val subscriberUrl = subscriberUrls.generateOne
      val otherSources  = microserviceBaseUrls.generateNonEmptyList(minElements = 3)
      upsertSubscriber(subscriberUrl, microserviceBaseUrl)

      otherSources.map(upsertSubscriber(subscriberUrl, _))

      val activeSource = otherSources.toList(otherSources.size / 2)
      otherSources.map {
        case `activeSource` => (serviceHealthChecker.ping _).expects(activeSource).returning(true.pure[IO])
        case inactive       => (serviceHealthChecker.ping _).expects(inactive).returning(false.pure[IO])
      }

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs List(
        subscriberUrl -> microserviceBaseUrl,
        subscriberUrl -> activeSource
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

  private def findAllSubscribers(): List[(SubscriberUrl, MicroserviceBaseUrl)] = execute {
    sql"""|SELECT DISTINCT delivery_url, source_url
          |FROM subscriber
          |""".stripMargin
      .query[(SubscriberUrl, MicroserviceBaseUrl)]
      .to[List]
  }
}
