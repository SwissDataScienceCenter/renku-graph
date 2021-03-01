package io.renku.eventlog.subscriptions.zombieevents

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.metrics.TestLabeledHistogram
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.wordspec.AnyWordSpec
import eu.timepit.refined.auto._
import io.renku.eventlog.subscriptions.Generators.subscriberUrls
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.microservices.MicroserviceBaseUrl
import org.scalatest.matchers.should
import doobie.implicits._

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
        (serviceHealth.ping _).expects(_).returning(true.pure[IO])
      }

      cleaner.removeZombieSources().unsafeRunSync() shouldBe ()

      findAllSubscribers() should contain theSameElementsAs (otherSources.map(sourceUrl =>
        subscriberUrl -> sourceUrl
      ) :+ (subscriberUrl -> microserviceBaseUrl)).toList
    }

  }

  private trait TestCase {
    val queriesExecTimes    = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val microserviceBaseUrl = microserviceBaseUrls.generateOne
    val serviceHealth       = mock[ServiceHealth[IO]]
    val cleaner             = new ZombieEventSourceCleanerImpl(transactor, queriesExecTimes, microserviceBaseUrl, serviceHealth)

  }

  private def findAllSubscribers(): List[(SubscriberUrl, MicroserviceBaseUrl)] = execute {
    sql"""|SELECT DISTINCT delivery_url, source_url
          |FROM subscriber
         """.stripMargin
      .query[(SubscriberUrl, MicroserviceBaseUrl)]
      .to[List]
  }
}
