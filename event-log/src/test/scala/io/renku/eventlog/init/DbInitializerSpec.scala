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

package io.renku.eventlog.init

import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.testtools.MockedRunnableCollaborators
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DbInitializerSpec extends AnyWordSpec with MockedRunnableCollaborators with MockFactory with should.Matchers {

  "run" should {

    "succeed if all the migration processes run fine" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      given(batchDateAdder).succeeds(returning = ())
      given(viewRemover).succeeds(returning = ())
      given(projectTableCreator).succeeds(returning = ())
      given(projectPathRemover).succeeds(returning = ())
      given(eventLogTableRenamer).succeeds(returning = ())
      given(eventStatusRenamer).succeeds(returning = ())

      dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "fail if creating event_log table fails" in new TestCase {

      val exception = exceptions.generateOne
      given(eventLogTableCreator).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if adding the project_path column fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(projectPathAdder).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if adding the batch_date column fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(batchDateAdder).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if dropping the latest event dates view fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      given(batchDateAdder).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(viewRemover).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if creating the project table fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      given(batchDateAdder).succeeds(returning = ())
      given(viewRemover).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(projectTableCreator).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if dropping the project_path column fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      given(batchDateAdder).succeeds(returning = ())
      given(viewRemover).succeeds(returning = ())
      given(projectTableCreator).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(projectPathRemover).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if renaming the event_log table fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      given(batchDateAdder).succeeds(returning = ())
      given(viewRemover).succeeds(returning = ())
      given(projectTableCreator).succeeds(returning = ())
      given(projectPathRemover).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(eventLogTableRenamer).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if renaming the processing status fails" in new TestCase {

      given(eventLogTableCreator).succeeds(returning = ())
      given(projectPathAdder).succeeds(returning = ())
      given(batchDateAdder).succeeds(returning = ())
      given(viewRemover).succeeds(returning = ())
      given(projectTableCreator).succeeds(returning = ())
      given(projectPathRemover).succeeds(returning = ())
      given(eventLogTableRenamer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(eventStatusRenamer).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }

  }

  private trait TestCase {
    val eventLogTableCreator = mock[EventLogTableCreator[IO]]
    val projectPathAdder     = mock[ProjectPathAdder[IO]]
    val batchDateAdder       = mock[BatchDateAdder[IO]]
    val viewRemover          = mock[LatestEventDatesViewRemover[IO]]
    val projectTableCreator  = mock[ProjectTableCreator[IO]]
    val projectPathRemover   = mock[ProjectPathRemover[IO]]
    val eventLogTableRenamer = mock[EventLogTableRenamer[IO]]
    val eventStatusRenamer   = mock[EventStatusRenamer[IO]]
    val logger               = TestLogger[IO]()
    val dbInitializer = new DbInitializerImpl[IO](
      eventLogTableCreator,
      projectPathAdder,
      batchDateAdder,
      viewRemover,
      projectTableCreator,
      projectPathRemover,
      eventLogTableRenamer,
      eventStatusRenamer,
      logger
    )
  }
}
