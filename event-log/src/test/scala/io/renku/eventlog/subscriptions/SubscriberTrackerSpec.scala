package io.renku.eventlog.subscriptions

import cats.effect.IO
import ch.datascience.db.SqlQuery
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.TestLabeledHistogram
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import eu.timepit.refined.auto._

class SubscriberTrackerSpec extends AnyWordSpec with InMemoryEventLogDbSpec with MockFactory with should.Matchers {
  "add" should {
    "insert a new row in the subscriber table if the subscriber does not exists" in new TestCase {}
    "insert a new row in the subscriber table fi the subscriber exists but the source_url is different" in new TestCase {}
    "do nothing if the subscriber info is already present in the table" in new TestCase {}
    "fail if the query fails" in new TestCase {}
  }

  "remove" should {
    "remove a subscriber if the subscriber and the current source url exists" in new TestCase {}
    "do nothing if the subscriber does not exists" in new TestCase {}
    "do nothing if the subscriber exists but the source_url is different than the current source url" in new TestCase {}
    "fail if the query fails" in new TestCase {}
  }

  private trait TestCase {
    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val logger           = TestLogger[IO]()
    val tracker          = new SubscriberTrackerImpl(transactor, queriesExecTimes, logger)
  }
}
