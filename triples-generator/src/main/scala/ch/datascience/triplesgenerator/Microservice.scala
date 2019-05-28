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
import ch.datascience.db.DbTransactorResource
import ch.datascience.dbeventlog.commands.IOEventLogFetch
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.dbeventlog.{EventLogDB, EventLogDbConfigProvider}
import ch.datascience.http.server.HttpServer
import ch.datascience.triplesgenerator.eventprocessing._
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGeneratorProvider
import ch.datascience.triplesgenerator.init._
import ch.datascience.triplesgenerator.reprovisioning.IOCompleteReProvisionEndpoint
import pureconfig._

import scala.concurrent.ExecutionContext

object Microservice extends IOApp {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(loadConfigOrThrow[Int]("threads-number"))

  protected implicit override def contextShift: ContextShift[IO] =
    IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] =
    IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new EventLogDbConfigProvider[IO] map DbTransactorResource[IO, EventLogDB]
      exitCode           <- runMicroservice(transactorResource, args)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, EventLogDB], args: List[String]) =
    transactorResource.use { transactor =>
      for {
        triplesGenerator <- new TriplesGeneratorProvider().get
        eventProcessorRunner <- new EventsSource[IO](DbEventProcessorRunner(_, new IOEventLogFetch(transactor)))
                                 .withEventsProcessor(new IOCommitEventProcessor(transactor, triplesGenerator))
        completeReProvisionEndpoint <- IOCompleteReProvisionEndpoint(transactor)
        exitCode <- new MicroserviceRunner(
                     new SentryInitializer[IO],
                     new IOEventLogDbInitializer(transactor),
                     new IOFusekiDatasetInitializer,
                     eventProcessorRunner,
                     new HttpServer[IO](serverPort    = 9002,
                                        serviceRoutes = new MicroserviceRoutes[IO](completeReProvisionEndpoint).routes)
                   ) run args
      } yield exitCode
    }
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
