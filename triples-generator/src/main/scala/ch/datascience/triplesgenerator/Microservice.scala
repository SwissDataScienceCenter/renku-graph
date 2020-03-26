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
import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.config.GitLab
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.db.DbTransactorResource
import ch.datascience.dbeventlog.commands.IOEventLogFetch
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.dbeventlog.{EventLogDB, EventLogDbConfigProvider}
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.eventprocessing._
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator
import ch.datascience.triplesgenerator.init._
import ch.datascience.triplesgenerator.metrics.{EventLogMetrics, IOEventLogMetrics}
import ch.datascience.triplesgenerator.reprovisioning.{IOReProvisioning, ReProvisioning}
import pureconfig._

import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

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
        sentryInitializer        <- SentryInitializer[IO]
        fusekiDatasetInitializer <- IOFusekiDatasetInitializer()
        triplesGeneration        <- TriplesGeneration[IO]()
        metricsRegistry          <- MetricsRegistry()
        gitLabRateLimit          <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
        gitLabThrottler          <- Throttler[IO, GitLab](gitLabRateLimit)
        sparqlTimeRecorder       <- SparqlQueryTimeRecorder(metricsRegistry)
        reProvisioning           <- IOReProvisioning(triplesGeneration, transactor, sparqlTimeRecorder)
        triplesGenerator         <- TriplesGenerator(triplesGeneration)
        routes                   <- new MicroserviceRoutes[IO](new RoutesMetrics[IO](metricsRegistry)).routes
        eventsFetcher            <- IOEventLogFetch(transactor)
        eventLogMetrics          <- IOEventLogMetrics(transactor, ApplicationLogger, metricsRegistry)
        commitEventProcessor <- IOCommitEventProcessor(transactor,
                                                       triplesGenerator,
                                                       metricsRegistry,
                                                       gitLabThrottler,
                                                       sparqlTimeRecorder)
        eventProcessorRunner <- new EventsSource[IO](DbEventProcessorRunner(_, eventsFetcher))
                                 .withEventsProcessor(commitEventProcessor)
        exitCode <- new MicroserviceRunner(
                     sentryInitializer,
                     new IOEventLogDbInitializer(transactor),
                     fusekiDatasetInitializer,
                     reProvisioning,
                     eventProcessorRunner,
                     eventLogMetrics,
                     new HttpServer[IO](serverPort = 9002, routes),
                     subProcessesCancelTokens
                   ) run args
      } yield exitCode
    }
}

private class MicroserviceRunner(
    sentryInitializer:        SentryInitializer[IO],
    eventLogDbInitializer:    IOEventLogDbInitializer,
    datasetInitializer:       FusekiDatasetInitializer[IO],
    reProvisioning:           ReProvisioning[IO],
    eventProcessorRunner:     EventProcessorRunner[IO],
    eventLogMetrics:          EventLogMetrics,
    httpServer:               HttpServer[IO],
    subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:      ContextShift[IO]) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _        <- sentryInitializer.run
      _        <- eventLogDbInitializer.run
      _        <- datasetInitializer.run
      _        <- reProvisioning.run.start.map(gatherCancelToken)
      _        <- eventProcessorRunner.run.start.map(gatherCancelToken)
      _        <- eventLogMetrics.run.start.map(gatherCancelToken)
      exitCode <- httpServer.run
    } yield exitCode

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
