/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository

import cats.MonadError
import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.tokenrepository.repository.init.IODbInitializer
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class MicroserviceRunnerSpec extends WordSpec with MockFactory {

  "run" should {

    "return Success Exit Code if Sentry initialization, db creation and http server start-up were successful" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (dbInitializer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if Sentry initialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (sentryInitializer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if db creation fails" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (dbInitializer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting http server fails" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (dbInitializer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      val exception = exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val sentryInitializer = mock[IOSentryInitializer]
    val dbInitializer     = mock[IODbInitializer]
    val httpServer        = mock[IOHttpServer]
    val runner            = new MicroserviceRunner(sentryInitializer, dbInitializer, httpServer)
  }
}
