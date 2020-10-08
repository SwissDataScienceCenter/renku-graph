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
import ch.datascience.metrics.{GaugeResetScheduler, IOGaugeResetScheduler, MetricsRegistry, RoutesMetrics}
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

object Microservice extends IOMicroservice {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new EventLogDbConfigProvider[IO] map DbTransactorResource[IO, EventLogDB]
      exitCode           <- runMicroservice(transactorResource)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, EventLogDB]) =
    transactorResource.use { transactor =>
      for {
        _                    <- new IODbInitializer(transactor, ApplicationLogger).run
        sentryInitializer    <- SentryInitializer[IO]()
        metricsRegistry      <- MetricsRegistry()
        queriesExecTimes     <- QueriesExecutionTimes(metricsRegistry)
        statsFinder          <- IOStatsFinder(transactor, queriesExecTimes)
        eventLogMetrics      <- IOEventLogMetrics(statsFinder, ApplicationLogger, metricsRegistry)
        waitingEventsGauge   <- WaitingEventsGauge(metricsRegistry, statsFinder, ApplicationLogger)
        underProcessingGauge <- UnderProcessingGauge(metricsRegistry, statsFinder, ApplicationLogger)
        gaugeScheduler <- IOGaugeResetScheduler(
                            List(waitingEventsGauge, underProcessingGauge),
                            new MetricsConfigProviderImpl[IO](),
                            ApplicationLogger
                          )
        eventCreationEndpoint <-
          IOEventCreationEndpoint(transactor, waitingEventsGauge, queriesExecTimes, ApplicationLogger)
        latestEventsEndpoint     <- IOLatestEventsEndpoint(transactor, queriesExecTimes, ApplicationLogger)
        processingStatusEndpoint <- IOProcessingStatusEndpoint(transactor, queriesExecTimes, ApplicationLogger)
        eventsPatchingEndpoint <- IOEventsPatchingEndpoint(transactor,
                                                           waitingEventsGauge,
                                                           underProcessingGauge,
                                                           queriesExecTimes,
                                                           ApplicationLogger
                                  )
        statusChangeEndpoint <- IOStatusChangeEndpoint(transactor,
                                                       waitingEventsGauge,
                                                       underProcessingGauge,
                                                       queriesExecTimes,
                                                       ApplicationLogger
                                )
        subscriptions <- Subscriptions(ApplicationLogger)
        eventsDispatcher <- EventsDispatcher(transactor,
                                             subscriptions,
                                             waitingEventsGauge,
                                             underProcessingGauge,
                                             queriesExecTimes,
                                             ApplicationLogger
                            )
        subscriptionsEndpoint <- IOSubscriptionsEndpoint(subscriptions, ApplicationLogger)
        microserviceRoutes = new MicroserviceRoutes[IO](
                               eventCreationEndpoint,
                               latestEventsEndpoint,
                               processingStatusEndpoint,
                               eventsPatchingEndpoint,
                               statusChangeEndpoint,
                               subscriptionsEndpoint,
                               new RoutesMetrics[IO](metricsRegistry)
                             ).routes
        exitcode <- microserviceRoutes.use { routes =>
                      val httpServer = new HttpServer[IO](serverPort = 9005, routes)

                      new MicroserviceRunner(
                        sentryInitializer,
                        eventLogMetrics,
                        eventsDispatcher,
                        gaugeScheduler,
                        httpServer,
                        subProcessesCancelTokens
                      ).run()
                    }
      } yield exitcode

    }

}

private class MicroserviceRunner(
    sentryInitializer:        SentryInitializer[IO],
    metrics:                  EventLogMetrics,
    eventsDispatcher:         EventsDispatcher,
    gaugeScheduler:           GaugeResetScheduler[IO],
    httpServer:               HttpServer[IO],
    subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:      ContextShift[IO]) {

  def run(): IO[ExitCode] =
    for {
      _      <- sentryInitializer.run()
      _      <- metrics.run().start map gatherCancelToken
      _      <- eventsDispatcher.run().start map gatherCancelToken
      _      <- gaugeScheduler.run().start map gatherCancelToken
      result <- httpServer.run()
    } yield result

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
