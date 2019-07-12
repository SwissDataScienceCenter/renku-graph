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

package ch.datascience.webhookservice

import cats.MonadError
import cats.effect._
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.webhookservice.missedevents._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class MicroserviceRunnerSpec extends WordSpec with MockFactory {

  "run" should {

    "return Success Exit Code if all " +
      "Sentry initialization and " +
      "Event Log db verification and " +
      "Events Synchronization Scheduler and " +
      "Http Server " +
      "start up successfully" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventsSynchronizationScheduler.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if Sentry initialization fails" in new TestCase {

      val exception = Generators.exceptions.generateOne
      (sentryInitializer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if Event Log db verification fails" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = Generators.exceptions.generateOne
      (eventLogDbInitializer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting up Events Synchronization Scheduler fails" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      val exception = Generators.exceptions.generateOne
      (eventsSynchronizationScheduler.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode regardless of Http Server start-up" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = Generators.exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(context.raiseError(exception))

      (eventsSynchronizationScheduler.run _)
        .expects()
        .returning(context.unit)

      runner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val sentryInitializer              = mock[IOSentryInitializer]
    val eventLogDbInitializer          = mock[IOEventLogDbInitializer]
    val eventsSynchronizationScheduler = mock[TestIOEventsSynchronizationScheduler]
    val httpServer                     = mock[IOHttpServer]
    val runner = new MicroserviceRunner(
      sentryInitializer,
      eventLogDbInitializer,
      eventsSynchronizationScheduler,
      httpServer
    )
  }
}
