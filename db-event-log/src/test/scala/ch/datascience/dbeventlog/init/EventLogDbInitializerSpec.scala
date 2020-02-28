/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.init

import cats.effect._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.dbeventlog._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{CommitEventId, CommittedDate}
import ch.datascience.graph.model.projects.Path
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class EventLogDbInitializerSpec extends WordSpec with DbInitSpec with MockFactory {

  "run" should {

    "do nothing if event_log table already exists" in new TestCase {
      if (!tableExists()) createTable()

      tableExists() shouldBe true

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)
      (batchDateAdder.run _)
        .expects()
        .returning(IO.unit)

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      tableExists() shouldBe true

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "create the event_log table if it does not exist" in new TestCase {
      if (tableExists()) dropTable()

      tableExists() shouldBe false

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)
      (batchDateAdder.run _)
        .expects()
        .returning(IO.unit)

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      tableExists() shouldBe true

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "ensure indexes are created for project_id, status and execution_date" in new TestCase {

      if (!tableExists()) createTable()

      tableExists() shouldBe true

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)
      (batchDateAdder.run _)
        .expects()
        .returning(IO.unit)

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      verifyTrue(sql"DROP INDEX idx_project_id;")
      verifyTrue(sql"DROP INDEX idx_status;")
      verifyTrue(sql"DROP INDEX idx_execution_date;")
      verifyTrue(sql"DROP INDEX idx_event_date;")
      verifyTrue(sql"DROP INDEX idx_created_date;")
    }

    "ensure TRIPLES_STORE_FAILURE to RECOVERABLE_FAILURE update is run" in new TestCase {

      if (!tableExists()) createTable()

      tableExists() shouldBe true

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)
      (batchDateAdder.run _)
        .expects()
        .returning(IO.unit)

      storeEvent()
      storeEvent(eventStatus = "TRIPLES_STORE_FAILURE")
      storeEvent()

      execute {
        sql"select count(*) from event_log where status = 'TRIPLES_STORE_FAILURE';".query[Int].unique
      } shouldBe 1

      dbInitializer.run.unsafeRunSync() shouldBe ((): Unit)

      execute {
        sql"select count(*) from event_log where status = 'TRIPLES_STORE_FAILURE';".query[Int].unique
      } shouldBe 0
    }

    "fails if adding the project_path column fails" in new TestCase {

      if (!tableExists()) createTable()

      tableExists() shouldBe true

      val exception = exceptions.generateOne
      (projectPathAdder.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        dbInitializer.run.unsafeRunSync()
      } shouldBe exception
    }

    "fails if adding the batch_date column fails" in new TestCase {

      if (!tableExists()) createTable()

      tableExists() shouldBe true

      (projectPathAdder.run _)
        .expects()
        .returning(IO.unit)
      val exception = exceptions.generateOne
      (batchDateAdder.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        dbInitializer.run.unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val projectPathAdder = mock[IOProjectPathAdder]
    val batchDateAdder   = mock[IOBatchDateAdder]
    val logger           = TestLogger[IO]()
    val dbInitializer    = new EventLogDbInitializer[IO](projectPathAdder, batchDateAdder, transactor, logger)
  }

  private class IOProjectPathAdder(transactor: DbTransactor[IO, EventLogDB], logger: Logger[IO])
      extends ProjectPathAdder[IO](transactor, logger)
  private class IOBatchDateAdder(transactor: DbTransactor[IO, EventLogDB], logger: Logger[IO])
      extends BatchDateAdder[IO](transactor, logger)

  private def storeEvent(commitEventId: CommitEventId = commitEventIds.generateOne,
                         eventStatus:   String        = eventStatuses.generateOne.value,
                         executionDate: ExecutionDate = executionDates.generateOne,
                         eventDate:     CommittedDate = committedDates.generateOne,
                         eventBody:     EventBody     = eventBodies.generateOne,
                         createdDate:   CreatedDate   = createdDates.generateOne,
                         projectPath:   Path          = projectPaths.generateOne): Unit = execute {
    sql"""|insert into 
          |event_log (event_id, project_id, status, created_date, execution_date, event_date, event_body) 
          |values (${commitEventId.id.value}, ${commitEventId.projectId.value}, $eventStatus, ${createdDate.value}, ${executionDate.value}, ${eventDate.value}, ${eventBody.value})
      """.stripMargin.update.run.map(_ => ())
  }
}
