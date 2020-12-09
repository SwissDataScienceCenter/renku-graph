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

import cats.effect._
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.db.DbTransactorResource
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics._
import ch.datascience.microservices.IOMicroservice
import io.renku.eventlog.creation.IOEventCreationEndpoint
import io.renku.eventlog.eventspatching.IOEventsPatchingEndpoint
import io.renku.eventlog.init.{DbInitializer, IODbInitializer}
import io.renku.eventlog.latestevents.IOLatestEventsEndpoint
import io.renku.eventlog.metrics._
import io.renku.eventlog.processingstatus.IOProcessingStatusEndpoint
import io.renku.eventlog.statuschange.IOStatusChangeEndpoint
import io.renku.eventlog.subscriptions._
import pureconfig.ConfigSource

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new EventLogDbConfigProvider[IO](args) map DbTransactorResource[IO, EventLogDB]
      exitCode           <- runMicroservice(transactorResource)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, EventLogDB]) =
    transactorResource.use { transactor =>
      for {
        certificateLoader           <- CertificateLoader[IO](ApplicationLogger)
        sentryInitializer           <- SentryInitializer[IO]()
        dbInitializer               <- IODbInitializer(transactor, ApplicationLogger)
        metricsRegistry             <- MetricsRegistry()
        queriesExecTimes            <- QueriesExecutionTimes(metricsRegistry)
        statsFinder                 <- IOStatsFinder(transactor, queriesExecTimes)
        eventLogMetrics             <- IOEventLogMetrics(statsFinder, ApplicationLogger, metricsRegistry)
        awaitingGenerationGauge     <- AwaitingGenerationGauge(metricsRegistry, statsFinder, ApplicationLogger)
        awaitingTransformationGauge <- AwaitingTransformationGauge(metricsRegistry, statsFinder, ApplicationLogger)
        underTransformationGauge    <- UnderTransformationGauge(metricsRegistry, statsFinder, ApplicationLogger)
        underTriplesGenerationGauge <- UnderTriplesGenerationGauge(metricsRegistry, statsFinder, ApplicationLogger)
        metricsResetScheduler <-
          IOGaugeResetScheduler(
            List(awaitingGenerationGauge,
                 underTriplesGenerationGauge,
                 awaitingTransformationGauge,
                 underTransformationGauge
            ),
            MetricsConfigProvider(),
            ApplicationLogger
          )
        eventCreationEndpoint <- IOEventCreationEndpoint(
                                   transactor,
                                   awaitingGenerationGauge,
                                   queriesExecTimes,
                                   ApplicationLogger
                                 )
        latestEventsEndpoint     <- IOLatestEventsEndpoint(transactor, queriesExecTimes, ApplicationLogger)
        processingStatusEndpoint <- IOProcessingStatusEndpoint(transactor, queriesExecTimes, ApplicationLogger)
        eventsPatchingEndpoint <- IOEventsPatchingEndpoint(transactor,
                                                           awaitingGenerationGauge,
                                                           underTriplesGenerationGauge,
                                                           queriesExecTimes,
                                                           ApplicationLogger
                                  )
        statusChangeEndpoint <- IOStatusChangeEndpoint(
                                  transactor,
                                  awaitingGenerationGauge,
                                  underTriplesGenerationGauge,
                                  awaitingTransformationGauge,
                                  underTransformationGauge,
                                  queriesExecTimes,
                                  ApplicationLogger
                                )
        subscriptionCategoryRegistry <- IOSubscriptionCategoryRegistry(transactor,
                                                                       awaitingGenerationGauge,
                                                                       underTriplesGenerationGauge,
                                                                       queriesExecTimes,
                                                                       ApplicationLogger
                                        )
        subscriptionsEndpoint <- IOSubscriptionsEndpoint(subscriptionCategoryRegistry, ApplicationLogger)
        microserviceRoutes =
          new MicroserviceRoutes[IO](
            eventCreationEndpoint,
            latestEventsEndpoint,
            processingStatusEndpoint,
            eventsPatchingEndpoint,
            statusChangeEndpoint,
            subscriptionsEndpoint,
            new RoutesMetrics[IO](metricsRegistry)
          ).routes
        exitCode <- microserviceRoutes.use { routes =>
                      val httpServer = new HttpServer[IO](serverPort = 9005, routes)

                      new MicroserviceRunner(
                        certificateLoader,
                        sentryInitializer,
                        dbInitializer,
                        eventLogMetrics,
                        subscriptionCategoryRegistry,
                        metricsResetScheduler,
                        httpServer,
                        subProcessesCancelTokens
                      ).run()
                    }
      } yield exitCode
    }
}

private class MicroserviceRunner(
    certificateLoader:            CertificateLoader[IO],
    sentryInitializer:            SentryInitializer[IO],
    dbInitializer:                DbInitializer[IO],
    metrics:                      EventLogMetrics,
    subscriptionCategoryRegistry: SubscriptionCategoryRegistry[IO],
    metricsResetScheduler:        GaugeResetScheduler[IO],
    httpServer:                   HttpServer[IO],
    subProcessesCancelTokens:     ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:          ContextShift[IO]) {

  def run(): IO[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- dbInitializer.run()
    _      <- metrics.run().start map gatherCancelToken
    _      <- metricsResetScheduler.run().start map gatherCancelToken
    _      <- subscriptionCategoryRegistry.run().start map gatherCancelToken
    result <- httpServer.run()
  } yield result

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
