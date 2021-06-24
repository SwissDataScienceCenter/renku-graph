/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.db.{SessionPoolResource, SessionResource}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics._
import ch.datascience.microservices.IOMicroservice
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.EventEndpoint
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics._
import io.renku.eventlog.processingstatus.IOProcessingStatusEndpoint
import io.renku.eventlog.subscriptions._
import natchez.Trace.Implicits.noop
import pureconfig.ConfigSource

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

  val ServicePort: Int Refined Positive = 9005

  protected implicit override val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)
  protected implicit override def timer:        Timer[IO]        = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] = for {
    sessionPoolResource <- new EventLogDbConfigProvider[IO]() map SessionPoolResource[IO, EventLogDB]
    exitCode            <- runMicroservice(sessionPoolResource)
  } yield exitCode

  private def runMicroservice(sessionPoolResource: Resource[IO, SessionResource[IO, EventLogDB]]) =
    sessionPoolResource.use { sessionResource =>
      for {
        certificateLoader           <- CertificateLoader[IO](ApplicationLogger)
        sentryInitializer           <- SentryInitializer[IO]()
        dbInitializer               <- DbInitializer(sessionResource, ApplicationLogger)
        metricsRegistry             <- MetricsRegistry()
        queriesExecTimes            <- QueriesExecutionTimes(metricsRegistry)
        statsFinder                 <- IOStatsFinder(sessionResource, queriesExecTimes)
        eventLogMetrics             <- IOEventLogMetrics(statsFinder, ApplicationLogger, metricsRegistry)
        awaitingGenerationGauge     <- AwaitingGenerationGauge(metricsRegistry, statsFinder)
        awaitingTransformationGauge <- AwaitingTransformationGauge(metricsRegistry, statsFinder)
        underTransformationGauge    <- UnderTransformationGauge(metricsRegistry, statsFinder)
        underTriplesGenerationGauge <- UnderTriplesGenerationGauge(metricsRegistry, statsFinder)
        metricsResetScheduler <- IOGaugeResetScheduler(
                                   List(awaitingGenerationGauge,
                                        underTriplesGenerationGauge,
                                        awaitingTransformationGauge,
                                        underTransformationGauge
                                   ),
                                   MetricsConfigProvider(),
                                   ApplicationLogger
                                 )
        creationSubscription <- events.categories.creation.SubscriptionFactory(sessionResource,
                                                                               awaitingGenerationGauge,
                                                                               queriesExecTimes,
                                                                               ApplicationLogger
                                )
        zombieEventsSubscription <- events.categories.zombieevents.SubscriptionFactory(
                                      sessionResource,
                                      awaitingGenerationGauge,
                                      underTriplesGenerationGauge,
                                      awaitingTransformationGauge,
                                      underTransformationGauge,
                                      queriesExecTimes,
                                      ApplicationLogger
                                    )
        commitSyncRequestSubscription <- events.categories.commitsyncrequest.SubscriptionFactory(
                                           sessionResource,
                                           queriesExecTimes,
                                           ApplicationLogger
                                         )
        eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                    creationSubscription,
                                    zombieEventsSubscription,
                                    commitSyncRequestSubscription
                                  )
        eventEndpoint            <- EventEndpoint(eventConsumersRegistry)
        processingStatusEndpoint <- IOProcessingStatusEndpoint(sessionResource, queriesExecTimes, ApplicationLogger)
        eventProducersRegistry <- EventProducersRegistry(
                                    sessionResource,
                                    awaitingGenerationGauge,
                                    underTriplesGenerationGauge,
                                    awaitingTransformationGauge,
                                    underTransformationGauge,
                                    queriesExecTimes,
                                    ApplicationLogger
                                  )
        subscriptionsEndpoint <- IOSubscriptionsEndpoint(eventProducersRegistry, ApplicationLogger)
        eventDetailsEndpoint  <- EventDetailsEndpoint(sessionResource, queriesExecTimes, ApplicationLogger)
        microserviceRoutes = new MicroserviceRoutes[IO](
                               eventEndpoint,
                               processingStatusEndpoint,
                               subscriptionsEndpoint,
                               eventDetailsEndpoint,
                               new RoutesMetrics[IO](metricsRegistry)
                             ).routes
        exitCode <- microserviceRoutes.use { routes =>
                      val httpServer = new HttpServer[IO](serverPort = ServicePort.value, routes)

                      new MicroserviceRunner(
                        certificateLoader,
                        sentryInitializer,
                        dbInitializer,
                        eventLogMetrics,
                        eventProducersRegistry,
                        eventConsumersRegistry,
                        metricsResetScheduler,
                        httpServer,
                        subProcessesCancelTokens
                      ).run()
                    }
      } yield exitCode
    }
}

private class MicroserviceRunner(
    certificateLoader:        CertificateLoader[IO],
    sentryInitializer:        SentryInitializer[IO],
    dbInitializer:            DbInitializer[IO],
    metrics:                  EventLogMetrics[IO],
    eventProducersRegistry:   EventProducersRegistry[IO],
    eventConsumersRegistry:   EventConsumersRegistry[IO],
    metricsResetScheduler:    GaugeResetScheduler[IO],
    httpServer:               HttpServer[IO],
    subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:      ContextShift[IO]) {

  def run(): IO[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- dbInitializer.run()
    _      <- metrics.run().start map gatherCancelToken
    _      <- metricsResetScheduler.run().start map gatherCancelToken
    _      <- eventProducersRegistry.run().start map gatherCancelToken
    _      <- eventConsumersRegistry.run().start map gatherCancelToken
    result <- httpServer.run()
  } yield result

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
