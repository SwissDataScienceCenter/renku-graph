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

package io.renku.eventlog.events.producers.zombieevents

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators._
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.ServiceHealthChecker
import io.renku.microservices.MicroserviceBaseUrl
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk._
import skunk.implicits._

class ZombieNodesCleanerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "removeZombieNodes" should {

    "do nothing if there are no rows in the subscriber table" in testDBResource.use { implicit cfg =>
      cleaner.removeZombieNodes().assertNoException >>
        findAllSubscribers.asserting(_ shouldBe List())
    }

    "do nothing if all sources and subscribers listed in the subscriber table are up" in testDBResource.use {
      implicit cfg =>
        val subscriber1Id  = subscriberIds.generateOne
        val subscriber1Url = subscriberUrls.generateOne
        givenHealthChecking(subscriber1Url.toUnsafe[MicroserviceBaseUrl], returning = true.pure[IO])
        for {
          _ <- upsertSubscriber(subscriber1Id, subscriber1Url, microserviceBaseUrl)

          subscriber2Id  = subscriberIds.generateOne
          subscriber2Url = subscriberUrls.generateOne
          _              = givenHealthChecking(subscriber2Url.toUnsafe[MicroserviceBaseUrl], returning = true.pure[IO])
          _ <- upsertSubscriber(subscriber2Id, subscriber2Url, microserviceBaseUrl)

          otherSources = microserviceBaseUrls.generateNonEmptyList().toList
          _ <- otherSources.map(upsertSubscriber(subscriber1Id, subscriber1Url, _)).sequence

          _ = otherSources.map(givenHealthChecking(_, returning = true.pure[IO]))

          _ <- cleaner.removeZombieNodes().assertNoException

          _ <- findAllSubscribers.asserting {
                 _ should contain theSameElementsAs
                   (otherSources :+ microserviceBaseUrl)
                     .map(FoundSubscriber(subscriber1Id, subscriber1Url, _)) :+ FoundSubscriber(subscriber2Id,
                                                                                                subscriber2Url,
                                                                                                microserviceBaseUrl
                   )
               }
        } yield Succeeded
    }

    "remove rows from the subscriber table if both the sources and the deliveries are inactive " +
      "but move active subscribers on inactive source to the current source" in testDBResource.use { implicit cfg =>
        val activeSubscriberId  = subscriberIds.generateOne
        val activeSubscriberUrl = subscriberUrls.generateOne
        givenHealthChecking(activeSubscriberUrl.toUnsafe[MicroserviceBaseUrl], returning = true.pure[IO])
        for {
          _ <- upsertSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)

          anotherActiveSubscriberId  = subscriberIds.generateOne
          anotherActiveSubscriberUrl = subscriberUrls.generateOne
          _ = givenHealthChecking(anotherActiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl], returning = true.pure[IO])

          inactiveSubscriberId  = subscriberIds.generateOne
          inactiveSubscriberUrl = subscriberUrls.generateOne
          _ = givenHealthChecking(inactiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl], returning = false.pure[IO])
          _ <- upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, microserviceBaseUrl)

          otherSources = microserviceBaseUrls.generateNonEmptyList(min = 3).toList
          _ <- otherSources.map(upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, _)).sequence

          someInactiveSource = otherSources.head
          _ <- upsertSubscriber(anotherActiveSubscriberId, anotherActiveSubscriberUrl, someInactiveSource)
          _ <- upsertSubscriber(activeSubscriberId, activeSubscriberUrl, someInactiveSource)

          activeSource = otherSources(otherSources.size / 2)
          _ = otherSources.map {
                case `activeSource` => givenHealthChecking(activeSource, returning = true.pure[IO])
                case inactive       => givenHealthChecking(inactive, returning = false.pure[IO])
              }
          _ <- upsertSubscriber(activeSubscriberId, activeSubscriberUrl, activeSource)

          _ <- cleaner.removeZombieNodes().assertNoException

          _ <- findAllSubscribers.asserting {
                 _ should contain theSameElementsAs List(
                   FoundSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl),
                   FoundSubscriber(activeSubscriberId, activeSubscriberUrl, activeSource),
                   FoundSubscriber(anotherActiveSubscriberId, anotherActiveSubscriberUrl, microserviceBaseUrl)
                 )
               }
        } yield Succeeded
      }

    "remove rows from the subscriber table if there are inactive subscriber on the current source" in testDBResource
      .use { implicit cfg =>
        val activeSubscriberId  = subscriberIds.generateOne
        val activeSubscriberUrl = subscriberUrls.generateOne
        givenHealthChecking(activeSubscriberUrl.toUnsafe[MicroserviceBaseUrl], returning = true.pure[IO])
        for {
          _ <- upsertSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)

          inactiveSubscriberId  = subscriberIds.generateOne
          inactiveSubscriberUrl = subscriberUrls.generateOne
          _ = givenHealthChecking(inactiveSubscriberUrl.toUnsafe[MicroserviceBaseUrl], returning = false.pure[IO])
          _ <- upsertSubscriber(inactiveSubscriberId, inactiveSubscriberUrl, microserviceBaseUrl)

          _ <- cleaner.removeZombieNodes().assertNoException

          _ <- findAllSubscribers.asserting {
                 _ should contain theSameElementsAs List(
                   FoundSubscriber(activeSubscriberId, activeSubscriberUrl, microserviceBaseUrl)
                 )
               }
        } yield Succeeded
      }
  }

  private val microserviceBaseUrl  = microserviceBaseUrls.generateOne
  private val serviceHealthChecker = mock[ServiceHealthChecker[IO]]
  private def cleaner(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new ZombieNodesCleanerImpl(microserviceBaseUrl, serviceHealthChecker)
  }

  private def givenHealthChecking(url: MicroserviceBaseUrl, returning: IO[Boolean]) =
    (serviceHealthChecker.ping _)
      .expects(url)
      .returning(returning)
      .atLeastOnce()

  private case class FoundSubscriber(id: SubscriberId, url: SubscriberUrl, microserviceUrl: MicroserviceBaseUrl)

  private def findAllSubscribers(implicit cfg: DBConfig[EventLogDB]): IO[List[FoundSubscriber]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[Void, FoundSubscriber] = sql"""
          SELECT DISTINCT delivery_id, delivery_url, source_url
          FROM subscriber"""
        .query(subscriberIdDecoder ~ subscriberUrlDecoder ~ microserviceBaseUrlDecoder)
        .map { case (id: SubscriberId) ~ (url: SubscriberUrl) ~ (microserviceUrl: MicroserviceBaseUrl) =>
          FoundSubscriber(id, url, microserviceUrl)
        }
      session.execute(query)
    }
}
