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

package ch.datascience.triplesgenerator

import java.util.concurrent.ConcurrentHashMap

import cats.effect._
import ch.datascience.dbeventlog.commands.EventLogStats
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.triplesgenerator.eventprocessing.IOEventProcessorRunner
import ch.datascience.triplesgenerator.init.IOFusekiDatasetInitializer
import ch.datascience.triplesgenerator.metrics.EventLogMetrics
import ch.datascience.triplesgenerator.reprovisioning.IOReProvisioning
import io.chrisdavenport.log4cats.Logger
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext

class MicroserviceRunnerSpec extends WordSpec with MockFactory {

  "run" should {

    "return Success ExitCode if " +
      "Sentry, Event Log db and RDF dataset initialize fine and " +
      "the re-provisioning, the Event Processor and the http server start up" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventLogDbInitializer.run _)
        .expects()
        .returning(IO.unit)

      (datasetInitializer.run _)
        .expects()
        .returning(IO.unit)

      (reProvisioning.run _)
        .expects()
        .returning(IO.unit)

      (eventProcessorRunner.run _)
        .expects()
        .returning(IO.unit)

      (eventLogMetrics.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(IO.pure(ExitCode.Success))

      microserviceRunner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if Sentry initialization fails" in new TestCase {
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

    "fail if RDF dataset verification fails" in new TestCase {
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

    "fail if starting the Http Server fails" in new TestCase {
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

      (eventLogMetrics.run _)
        .expects()
        .returning(IO.unit)

      (reProvisioning.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(IO.raiseError(exception))

      intercept[Exception] {
        microserviceRunner.run(Nil).unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode even if Event Processor fails" in new TestCase {
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

      (eventLogMetrics.run _)
        .expects()
        .returning(IO.unit)

      (reProvisioning.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(IO.pure(ExitCode.Success))

      microserviceRunner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if Event Log Metrics fails" in new TestCase {
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
      (eventLogMetrics.run _)
        .expects()
        .returning(IO.raiseError(exception))

      (reProvisioning.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(IO.pure(ExitCode.Success))

      microserviceRunner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if starting the http Server fails" in new TestCase {
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

      (eventLogMetrics.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(IO.pure(ExitCode.Success))

      val exception = exceptions.generateOne
      (reProvisioning.run _)
        .expects()
        .returning(IO.raiseError(exception))

      microserviceRunner.run(Nil).unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private trait TestCase {
    val sentryInitializer     = mock[IOSentryInitializer]
    val eventLogDbInitializer = mock[IOEventLogDbInitializer]
    val datasetInitializer    = mock[IOFusekiDatasetInitializer]
    val reProvisioning        = mock[IOReProvisioning]
    val eventProcessorRunner  = mock[IOEventProcessorRunner]
    val eventLogMetrics       = mock[IOEventLogMetrics]
    val httpServer            = mock[IOHttpServer]
    val microserviceRunner = new MicroserviceRunner(
      sentryInitializer,
      eventLogDbInitializer,
      datasetInitializer,
      reProvisioning,
      eventProcessorRunner,
      eventLogMetrics,
      httpServer,
      new ConcurrentHashMap[CancelToken[IO], Unit]()
    )
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]        = IO.timer(ExecutionContext.global)

  class IOEventLogMetrics(eventLogStats: EventLogStats[IO], logger: Logger[IO])
      extends EventLogMetrics[IO](eventLogStats, logger)
}
