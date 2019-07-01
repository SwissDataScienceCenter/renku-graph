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

package ch.datascience.triplesgenerator

import cats.effect.{ContextShift, ExitCode, IO}
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.triplesgenerator.eventprocessing.IOEventProcessorRunner
import ch.datascience.triplesgenerator.init.IOFusekiDatasetInitializer
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext

class MicroserviceRunnerSpec extends WordSpec with MockFactory {

  "run" should {

    "return Success ExitCode if " +
      "sentry initialization, " +
      "Event Log db verification, " +
      "dataset verification, " +
      "starting Event Processor and " +
      "http server succeeds" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      (datasetInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventProcessorRunner.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(IO.pure(ExitCode.Success))

      microserviceRunner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if sentry initialization fails" in new TestCase {
      val exception = exceptions.generateOne
      (sentryInitializer.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        microserviceRunner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if Event Log db verification fails" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        microserviceRunner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if dataset verification fails" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (datasetInitializer.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        microserviceRunner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting Event Processor fails" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      (datasetInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (eventProcessorRunner.run _)
        .expects()
        .returning(IO.raiseError(exception))

      (httpServer.run _)
        .expects()
        .returning(IO.pure(ExitCode.Success))

      intercept[Exception] {
        microserviceRunner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode regardless of Http Server start-up" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      (datasetInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventProcessorRunner.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(IO.raiseError(exception))

      microserviceRunner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private trait TestCase {
    val sentryInitializer     = mock[IOSentryInitializer]
    val eventLogDbInitializer = mock[IOEventLogDbInitializer]
    val datasetInitializer    = mock[IOFusekiDatasetInitializer]
    val eventProcessorRunner  = mock[IOEventProcessorRunner]
    val httpServer            = mock[IOHttpServer]
    val microserviceRunner = new MicroserviceRunner(
      sentryInitializer,
      eventLogDbInitializer,
      datasetInitializer,
      eventProcessorRunner,
      httpServer
    )
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
}
