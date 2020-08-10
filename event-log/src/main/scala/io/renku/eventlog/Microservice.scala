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

package io.renku.eventlog

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.db.DbTransactorResource
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import io.renku.eventlog.creation.IOEventCreationEndpoint
import io.renku.eventlog.eventspatching.IOEventsPatchingEndpoint
import io.renku.eventlog.init.IODbInitializer
import io.renku.eventlog.latestevents.IOLatestEventsEndpoint
import io.renku.eventlog.metrics._
import io.renku.eventlog.processingstatus.IOProcessingStatusEndpoint
import io.renku.eventlog.statuschange.IOStatusChangeEndpoint
import io.renku.eventlog.subscriptions.{EventsDispatcher, IOSubscriptionsEndpoint, Subscriptions}
import pureconfig.ConfigSource

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object Microservice extends IOMicroservice {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new EventLogDbConfigProvider[IO] map DbTransactorResource[IO, EventLogDB]
      exitCode           <- runMicroservice(transactorResource, args)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, EventLogDB], args: List[String]) =
    transactorResource.use { transactor =>
      val serviceRunner = for {
        _                    <- new IODbInitializer(transactor, ApplicationLogger).run.asResource
        sentryInitializer    <- SentryInitializer[IO].asResource
        metricsRegistry      <- MetricsRegistry().asResource
        queriesExecTimes     <- QueriesExecutionTimes(metricsRegistry).asResource
        statsFinder          <- IOStatsFinder(transactor, queriesExecTimes).asResource
        eventLogMetrics      <- IOEventLogMetrics(statsFinder, ApplicationLogger, metricsRegistry).asResource
        waitingEventsGauge   <- WaitingEventsGauge(metricsRegistry, statsFinder, ApplicationLogger).asResource
        underProcessingGauge <- UnderProcessingGauge(metricsRegistry, statsFinder, ApplicationLogger).asResource
        eventCreationEndpoint <- IOEventCreationEndpoint(transactor,
                                                         waitingEventsGauge,
                                                         queriesExecTimes,
                                                         ApplicationLogger).asResource
        latestEventsEndpoint     <- IOLatestEventsEndpoint(transactor, queriesExecTimes, ApplicationLogger).asResource
        processingStatusEndpoint <- IOProcessingStatusEndpoint(transactor, queriesExecTimes, ApplicationLogger).asResource
        eventsPatchingEndpoint <- IOEventsPatchingEndpoint(transactor,
                                                           waitingEventsGauge,
                                                           underProcessingGauge,
                                                           queriesExecTimes,
                                                           ApplicationLogger).asResource
        statusChangeEndpoint <- IOStatusChangeEndpoint(transactor,
                                                       waitingEventsGauge,
                                                       underProcessingGauge,
                                                       queriesExecTimes,
                                                       ApplicationLogger).asResource
        subscriptions <- Subscriptions(ApplicationLogger).asResource
        eventsDispatcher <- EventsDispatcher(transactor,
                                             subscriptions,
                                             waitingEventsGauge,
                                             underProcessingGauge,
                                             queriesExecTimes,
                                             ApplicationLogger).asResource
        subscriptionsEndpoint <- IOSubscriptionsEndpoint(subscriptions, ApplicationLogger).asResource
        routes <- new MicroserviceRoutes[IO](
                   eventCreationEndpoint,
                   latestEventsEndpoint,
                   processingStatusEndpoint,
                   eventsPatchingEndpoint,
                   statusChangeEndpoint,
                   subscriptionsEndpoint,
                   new RoutesMetrics[IO](metricsRegistry)
                 ).routes
        httpServer = new HttpServer[IO](serverPort = 9005, routes)

        exitCode = new MicroserviceRunner(
          sentryInitializer,
          eventLogMetrics,
          eventsDispatcher,
          httpServer,
          subProcessesCancelTokens
        ) run args
      } yield exitCode
      serviceRunner.use(identity)
    }

}

private class MicroserviceRunner(
    sentryInitializer:        SentryInitializer[IO],
    metrics:                  EventLogMetrics,
    eventsDispatcher:         EventsDispatcher,
    httpServer:               HttpServer[IO],
    subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:      ContextShift[IO]) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _      <- sentryInitializer.run
      _      <- metrics.run.start map gatherCancelToken
      _      <- eventsDispatcher.run.start map gatherCancelToken
      result <- httpServer.run
    } yield result

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
