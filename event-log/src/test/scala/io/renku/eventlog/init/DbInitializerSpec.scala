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

      given(migrator1).succeeds(returning = ())
      given(migrator2).succeeds(returning = ())

      dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("Event Log database initialization success"))
    }

    "fail if of the migrators fails" in new TestCase {

      given(migrator1).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(migrator2).fails(becauseOf = exception)

      intercept[Exception] {
        dbInitializer.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    import DbInitializer.Runnable

    val migrator1 = mock[EventLogTableCreator[IO]]
    val migrator2 = mock[EventPayloadTableCreator[IO]]
    val logger    = TestLogger[IO]()

    val dbInitializer = new DbInitializerImpl[IO](
      List[Runnable[IO, Unit]](migrator1, migrator2),
      logger
    )
  }
}
