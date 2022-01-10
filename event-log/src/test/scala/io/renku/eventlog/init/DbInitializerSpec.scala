/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.{IOSpec, MockedRunnableCollaborators}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class DbInitializerSpec
    extends AnyWordSpec
    with IOSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "succeed if all the migration processes run fine" +
      "set and unset the value of isMigrating before/after migrating" in new TestCase {
        (isMigrating.update _)
          .expects(where((f: Boolean => Boolean) => f(true) == true && f(false) == true))
          .returning(IO.unit)

        given(migrator1).succeeds(returning = ())
        given(migrator2).succeeds(returning = ())

        (isMigrating.update _)
          .expects(where((f: Boolean => Boolean) => f(true) == false && f(false) == false))
          .returning(IO.unit)

        dbInitializer.run().unsafeRunSync() shouldBe ((): Unit)

        logger.loggedOnly(Info("Event Log database initialization success"))
      }

    "retry if one of the migrators fails" in new TestCase {
      val exception = exceptions.generateOne
      inSequence {
        (isMigrating.update _)
          .expects(where((f: Boolean => Boolean) => f(true) == true && f(false) == true))
          .returning(IO.unit)

        given(migrator1).succeeds(returning = ())
        given(migrator2).fails(becauseOf = exception)

        (isMigrating.update _)
          .expects(where((f: Boolean => Boolean) => f(true) == true && f(false) == true))
          .returning(IO.unit)

        given(migrator1).succeeds(returning = ())
        given(migrator2).succeeds(returning = ())

        (isMigrating.update _)
          .expects(where((f: Boolean => Boolean) => f(true) == false && f(false) == false))
          .returning(IO.unit)

      }
      dbInitializer.run().unsafeRunSync()

      logger.loggedOnly(Error("Event Log database initialization failed: retrying 1 time(s)", exception),
                        Info("Event Log database initialization success")
      )
    }
  }

  private trait TestCase {

    import DbInitializer.Runnable

    val migrator1   = mock[EventLogTableCreator[IO]]
    val migrator2   = mock[EventPayloadTableCreator[IO]]
    val isMigrating = mock[Ref[IO, Boolean]]

    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    val dbInitializer = new DbInitializerImpl[IO](List[Runnable[IO, Unit]](migrator1, migrator2),
                                                  isMigrating,
                                                  retrySleepDuration = 0.5.seconds
    )
  }
}
