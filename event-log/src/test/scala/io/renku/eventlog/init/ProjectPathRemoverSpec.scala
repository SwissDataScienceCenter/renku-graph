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

package io.renku.eventlog.init

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectPathRemoverSpec
    extends AnyWordSpec
    with DbInitSpec
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  protected override lazy val migrationsToRun: List[Migration] = List(
    eventLogTableCreator,
    projectPathAdder,
    batchDateAdder,
    latestEventDatesViewRemover,
    projectTableCreator
  )

  "run" should {

    "do nothing if the 'event' table already exists" in new TestCase {

      createEventTable()

      projectPathRemover.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project_path' column dropping skipped"))
    }

    "remove the 'project_path' column if it exists on the 'event_log' table" in new TestCase {

      checkColumnExists shouldBe true

      projectPathRemover.run().unsafeRunSync() shouldBe ((): Unit)

      checkColumnExists shouldBe false

      logger.loggedOnly(Info("'project_path' column removed"))

      logger.reset()

      projectPathRemover.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project_path' column already removed"))
    }
  }

  private trait TestCase {
    val logger             = TestLogger[IO]()
    val projectPathRemover = new ProjectPathRemoverImpl[IO](transactor, logger)
  }

  private def checkColumnExists: Boolean =
    sql"select project_path from event_log limit 1"
      .query[String]
      .option
      .transact(transactor.resource)
      .map(_ => true)
      .recover { case _ => false }
      .unsafeRunSync()
}
