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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.TestLabeledHistogram
import doobie.implicits.toSqlInterpolator
import eu.timepit.refined.auto._
import io.renku.eventlog.InMemoryEventLogDbSpec
import io.renku.eventlog.subscriptions.Generators.subscriberUrls
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriberTrackerSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {
  "add" should {
    "insert a new row in the subscriber table if the subscriber does not exists" in new TestCase {

      findSubscriber(subscriberUrl, sourceUrl) shouldBe None

      tracker add subscriberUrl shouldBe true.pure[IO]

      findSubscriber(subscriberUrl, sourceUrl) shouldBe Some(subscriberUrl, sourceUrl)
    }
    "insert a new row in the subscriber table if the subscriber exists but the source_url is different" in new TestCase {
      val otherSource  = SubscriberUrl(nonEmptyStrings().generateOne)
      val otherTracker = new SubscriberTrackerImpl(transactor, queriesExecTimes, otherSource, logger)
      otherTracker add subscriberUrl shouldBe true.pure[IO]

      findSubscriber(subscriberUrl, otherSource) shouldBe Some(subscriberUrl, otherSource)
      findSubscriber(subscriberUrl, sourceUrl)   shouldBe None

      tracker add subscriberUrl shouldBe true.pure[IO]

      findSubscriber(subscriberUrl, otherSource) shouldBe Some(subscriberUrl, otherSource)
      findSubscriber(subscriberUrl, sourceUrl)   shouldBe Some(subscriberUrl, sourceUrl)
    }
    "do nothing if the subscriber info is already present in the table" in new TestCase {
      findSubscriber(subscriberUrl, sourceUrl) shouldBe None

      tracker add subscriberUrl                shouldBe true.pure[IO]
      findSubscriber(subscriberUrl, sourceUrl) shouldBe Some(subscriberUrl, sourceUrl)

      tracker add subscriberUrl                shouldBe true.pure[IO]
      findSubscriber(subscriberUrl, sourceUrl) shouldBe Some(subscriberUrl, sourceUrl)
    }

  }

  "remove" should {
    "remove a subscriber if the subscriber and the current source url exists" in new TestCase {
      storeSubscriberUrl(subscriberUrl, sourceUrl)

      findSubscriber(subscriberUrl, sourceUrl) shouldBe Some(subscriberUrl, sourceUrl)
      tracker remove subscriberUrl             shouldBe true.pure[IO]
      findSubscriber(subscriberUrl, sourceUrl) shouldBe None

    }
    "do nothing if the subscriber does not exists" in new TestCase {
      tracker remove subscriberUrl             shouldBe true.pure[IO]
      findSubscriber(subscriberUrl, sourceUrl) shouldBe None
    }

    "do nothing if the subscriber exists but the source_url is different than the current source url" in new TestCase {

      val otherSource: SubscriberUrl = subscriberUrls.generateOne
      storeSubscriberUrl(subscriberUrl, otherSource)
      findSubscriber(subscriberUrl, otherSource) shouldBe Some(subscriberUrl, otherSource)
      tracker remove subscriberUrl               shouldBe true.pure[IO]
      findSubscriber(subscriberUrl, otherSource) shouldBe Some(subscriberUrl, otherSource)
      findSubscriber(subscriberUrl, sourceUrl)   shouldBe None
    }
  }

  private trait TestCase {

    val subscriberUrl    = subscriberUrls.generateOne
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val logger           = TestLogger[IO]()
    val sourceUrl        = SubscriberUrl(nonEmptyStrings().generateOne)
    val tracker          = new SubscriberTrackerImpl(transactor, queriesExecTimes, sourceUrl, logger)
  }

  private def findSubscriber(subscriberUrl: SubscriberUrl,
                             sourceUrl:     SubscriberUrl
  ): Option[(SubscriberUrl, SubscriberUrl)] =
    execute {
      sql"""|SELECT delivery_url, source_url
            |FROM subscriber
            |WHERE delivery_url = ${subscriberUrl.value} AND source_url = ${sourceUrl.value};""".stripMargin
        .query[(SubscriberUrl, SubscriberUrl)]
        .option
    }

  private def storeSubscriberUrl(subscriberUrl: SubscriberUrl, sourceUrl: SubscriberUrl) = execute {
    sql"""|INSERT INTO
          |subscriber (delivery_url, source_url)
          |VALUES (${subscriberUrl.value}, ${sourceUrl.value})
      """.stripMargin.update.run.void
  }
}
