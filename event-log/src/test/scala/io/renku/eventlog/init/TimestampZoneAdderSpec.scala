package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TimestampZoneAdderSpec extends AnyWordSpec with DbInitSpec with should.Matchers {
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
    subscriberTableCreator,
    eventDeliveryTableCreator
  )

  "run" should {

    "modify the type of the timestamp columns" in new TestCase {

      tableExists("event") shouldBe true

      verify("batch_date", "timestamp")
      verify("create_date", "timestamp")
      verify("event_date", "timestamp")
      verify("execution_date", "timestamp")

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("batch_date", "timestampz")
      verify("create_date", "timestampz")
      verify("event_date", "timestampz")
      verify("execution_date", "timestampz")

    }

    "do nothing if the timestamp are already timestampz" in new TestCase {

      tableExists("event") shouldBe true

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("batch_date", "timestampz")
      verify("create_date", "timestampz")
      verify("event_date", "timestampz")
      verify("execution_date", "timestampz")

      tableRefactor.run().unsafeRunSync() shouldBe ((): Unit)

      verify("batch_date", "timestampz")
      verify("create_date", "timestampz")
      verify("event_date", "timestampz")
      verify("execution_date", "timestampz")

      logger.loggedOnly(Info("Fields are already in timestampz type"))

    }

  }

  private trait TestCase {
    val logger        = TestLogger[IO]()
    val tableRefactor = new TimestampZoneAdderImpl[IO](transactor, logger)
  }
}
