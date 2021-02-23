package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits.toSqlInterpolator
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDeliveryTableCreatorSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

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
    statusesProcessingTimeTableCreator
  )

  "run" should {

    "do nothing if the 'event_delivery' table already exists" in new TestCase {

      tableExists("event_delivery") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_delivery") shouldBe true

      logger.loggedOnly(Info("'event_delivery' table created"))

      logger.reset()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'event_delivery' table exists"))
    }

    "create indices for all the columns" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("event_delivery") shouldBe true

      verifyTrue(sql"DROP INDEX idx_event_id;")
      verifyTrue(sql"DROP INDEX idx_project_id;")
    }

  }

  private trait TestCase {
    val logger       = TestLogger[IO]()
    val tableCreator = new EventDeliveryTableCreatorImpl[IO](transactor, logger)
  }
}
