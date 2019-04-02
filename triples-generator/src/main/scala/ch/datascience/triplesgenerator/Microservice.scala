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

import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.dbeventlog.EventLogDbConfigProvider
import ch.datascience.dbeventlog.commands.IOEventLogFetch
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.http.server.HttpServer
import ch.datascience.triplesgenerator.eventprocessing._
import ch.datascience.triplesgenerator.init._
import pureconfig._

import scala.concurrent.ExecutionContext

object Microservice extends IOApp {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(loadConfigOrThrow[Int]("threads-number"))

  protected implicit override def contextShift: ContextShift[IO] =
    IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] =
    IO.timer(executionContext)

  private val microserviceInstantiator = for {
    eventLogDbConfig <- new EventLogDbConfigProvider[IO].get()
    eventLogDbInitializer = new IOEventLogDbInitializer(eventLogDbConfig)

    httpServer = new HttpServer[IO](
      serverPort = 9002,
      new MicroserviceRoutes[IO].routes
    )

    renkuLogTimeout <- new RenkuLogTimeoutConfigProvider[IO].get
    eventProcessorRunner = new EventsSource[IO](new DbEventProcessorRunner(_, new IOEventLogFetch(eventLogDbConfig)))
      .withEventsProcessor(new IOCommitEventProcessor(eventLogDbConfig, renkuLogTimeout))
  } yield
    new MicroserviceRunner(
      new SentryInitializer[IO],
      eventLogDbInitializer,
      new IOFusekiDatasetInitializer,
      eventProcessorRunner,
      httpServer
    )

  override def run(args: List[String]): IO[ExitCode] =
    for {
      microservice <- microserviceInstantiator
      exitCode     <- microservice.run(args)
    } yield exitCode
}

private class MicroserviceRunner(
    sentryInitializer:     SentryInitializer[IO],
    eventLogDbInitializer: IOEventLogDbInitializer,
    datasetInitializer:    FusekiDatasetInitializer[IO],
    eventProcessorRunner:  EventProcessorRunner[IO],
    httpServer:            HttpServer[IO]
)(implicit contextShift:   ContextShift[IO]) {

  import cats.implicits._

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- sentryInitializer.run
      _ <- eventLogDbInitializer.run
      _ <- datasetInitializer.run
      _ <- List(httpServer.run.start, eventProcessorRunner.run).sequence
    } yield ExitCode.Success
}
