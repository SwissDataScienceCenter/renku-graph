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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.{IOSpec, MockedRunnableCollaborators}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class DbInitializerSpec
    extends AnyWordSpec
    with MockFactory
    with IOSpec
    with should.Matchers
    with MockedRunnableCollaborators {

  import DbInitializer.Runnable

  "run" should {

    "run all the migrations" in new TestCase {
      given(migrator1).succeeds(returning = ())
      given(migrator2).succeeds(returning = ())

      initializer.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("Projects Tokens database initialization success"))
    }

    "retry if migration fails" in new TestCase {
      val exception = exceptions.generateOne
      inSequence {
        given(migrator1).succeeds(returning = ())
        given(migrator2).fails(becauseOf = exception)

        given(migrator1).succeeds(returning = ())
        given(migrator2).succeeds(returning = ())
      }

      initializer.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Error("Projects Tokens database initialization failed", exception),
                        Info("Projects Tokens database initialization success")
      )
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val migrator1 = mock[ProjectsTokensTableCreator[IO]]
    val migrator2 = mock[ProjectPathAdder[IO]]
    val initializer = new DbInitializerImpl(
      List[Runnable[IO, Unit]](migrator1, migrator2),
      retrySleepDuration = 500 millis
    )
  }
}
