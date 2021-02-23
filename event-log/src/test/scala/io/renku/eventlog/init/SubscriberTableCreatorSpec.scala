package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits.toSqlInterpolator
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SubscriberTableCreatorSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover,
    projectTableCreator,
    projectPathRemover,
    eventLogTableRenamer,
    eventStatusRenamer,
    eventPayloadTableCreator,
    eventPayloadSchemaVersionAdder,
    subscriptionCategorySyncTimeTableCreator,
    statusesProcessingTimeTableCreator,
    eventDeliveryTableCreator
  )

  "run" should {

    "do nothing if the 'subscriber' table already exists" in new TestCase {

      tableExists("subscriber") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("subscriber") shouldBe true

      logger.loggedOnly(Info("'subscriber' table created"))

      logger.reset()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'subscriber' table exists"))
    }

    "create indices for all the columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("subscriber") shouldBe true

      verifyTrue(sql"DROP INDEX idx_delivery_url;")
    }

  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new SubscriberTableCreatorImpl[IO](transactor, logger)
  }
}
