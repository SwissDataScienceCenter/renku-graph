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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.Generators._
import io.renku.events.Subscription
import io.renku.events.Subscription._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.microservices.MicroserviceBaseUrl
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}
import skunk._
import skunk.implicits._

class DefaultSubscriberTrackerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with OptionValues
    with should.Matchers {

  private val subscriber = subscribers.generateOne
  private val sourceUrl  = microserviceBaseUrls.generateOne

  "add" should {

    "insert a new row in the subscriber table if the subscriber does not exists" in testDBResource.use { implicit cfg =>
      for {
        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)

        _ <- (tracker add subscriber).asserting(_ shouldBe true)

        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
               _.value shouldBe (subscriber.id, subscriber.url, sourceUrl)
             )
      } yield Succeeded
    }

    "replace existing row for the subscription info with the same subscriber url but different subscriber id" in testDBResource
      .use { implicit cfg =>
        for {
          _ <- findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)

          _ <- (tracker add subscriber).asserting(_ shouldBe true)

          _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
                 _.value shouldBe (subscriber.id, subscriber.url, sourceUrl)
               )

          newSubscriberId = subscriberIds.generateOne
          _ <- (tracker add subscriber.fold(_.copy(id = newSubscriberId), _.copy(id = newSubscriberId)))
                 .asserting(_ shouldBe true)

          _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
                 _.value shouldBe (newSubscriberId, subscriber.url, sourceUrl)
               )
        } yield Succeeded
      }

    "insert a new row in the subscriber table " +
      "if the subscriber exists but the source_url is different" in testDBResource.use { implicit cfg =>
        val otherSource  = microserviceBaseUrls.generateOne
        val otherTracker = new DefaultSubscriberTrackerImpl[IO](otherSource)

        for {
          _ <- (otherTracker add subscriber).asserting(_ shouldBe true)

          _ <- findSubscriber(subscriber.url, otherSource).asserting(
                 _.value shouldBe (subscriber.id, subscriber.url, otherSource)
               )
          _ <- findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)

          _ <- (tracker add subscriber).asserting(_ shouldBe true)

          _ <- findSubscriber(subscriber.url, otherSource).asserting(
                 _.value shouldBe (subscriber.id, subscriber.url, otherSource)
               )
          _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
                 _.value shouldBe (subscriber.id, subscriber.url, sourceUrl)
               )
        } yield Succeeded
      }

    "do nothing if the subscriber info is already present in the table" in testDBResource.use { implicit cfg =>
      for {
        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)

        _ <- (tracker add subscriber).asserting(_ shouldBe true)
        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
               _.value shouldBe (subscriber.id, subscriber.url, sourceUrl)
             )

        _ <- (tracker add subscriber).asserting(_ shouldBe true)
        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
               _.value shouldBe (subscriber.id, subscriber.url, sourceUrl)
             )
      } yield Succeeded
    }
  }

  "remove" should {

    "remove a subscriber if the subscriber and the current source url exists" in testDBResource.use { implicit cfg =>
      for {
        _ <- storeSubscriptionInfo(subscriber, sourceUrl)

        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(
               _.value shouldBe (subscriber.id, subscriber.url, sourceUrl)
             )

        _ <- (tracker remove subscriber.url).asserting(_ shouldBe true)
        _ <- findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)
      } yield Succeeded
    }

    "do nothing if the subscriber does not exists" in testDBResource.use { implicit cfg =>
      (tracker remove subscriber.url).asserting(_ shouldBe true) >>
        findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)
    }

    "do nothing if the subscriber exists but the source_url is different than the current source url" in testDBResource
      .use { implicit cfg =>
        val otherSource = microserviceBaseUrls.generateOne
        for {
          _ <- storeSubscriptionInfo(subscriber, otherSource)
          _ <- findSubscriber(subscriber.url, otherSource).asserting(
                 _.value shouldBe (subscriber.id, subscriber.url, otherSource)
               )

          _ <- (tracker remove subscriber.url).asserting(_ shouldBe true)
          _ <- findSubscriber(subscriber.url, otherSource).asserting(
                 _.value shouldBe (subscriber.id, subscriber.url, otherSource)
               )

          _ <- findSubscriber(subscriber.url, sourceUrl).asserting(_ shouldBe None)
        } yield Succeeded
      }
  }

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private def tracker(implicit cfg: DBConfig[EventLogDB]) =
    new DefaultSubscriberTrackerImpl[IO](sourceUrl)

  private def findSubscriber(subscriberUrl: SubscriberUrl, sourceUrl: MicroserviceBaseUrl)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Option[(SubscriberId, SubscriberUrl, MicroserviceBaseUrl)]] =
    moduleSessionResource.session.use { session =>
      val query: Query[SubscriberUrl *: MicroserviceBaseUrl *: EmptyTuple,
                       (SubscriberId, SubscriberUrl, MicroserviceBaseUrl)
      ] = sql"""SELECT delivery_id, delivery_url, source_url
                FROM subscriber
                WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder"""
        .query(subscriberIdDecoder ~ subscriberUrlDecoder ~ microserviceBaseUrlDecoder)
        .map { case subscriberId ~ subscriberUrl ~ microserviceBaseUrl =>
          (subscriberId, subscriberUrl, microserviceBaseUrl)
        }

      session.prepare(query).flatMap(_.option(subscriberUrl *: sourceUrl *: EmptyTuple))
    }

  private def storeSubscriptionInfo(subscriber: Subscription.Subscriber, sourceUrl: MicroserviceBaseUrl)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource.session.use { session =>
      val query: Command[SubscriberId *: SubscriberUrl *: MicroserviceBaseUrl *: EmptyTuple] =
        sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
              VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
          """.command
      session
        .prepare(query)
        .flatMap(_.execute(subscriber.id *: subscriber.url *: sourceUrl *: EmptyTuple))
        .void
    }
}
