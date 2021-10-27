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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.db.{SessionPoolResource, SessionResource}
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.{EventEndpoint, EventsEndpoint}
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics._
import io.renku.eventlog.processingstatus.ProcessingStatusEndpoint
import io.renku.eventlog.subscriptions._
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.model.projects
import io.renku.http.server.HttpServer
import io.renku.logging.ApplicationLogger
import io.renku.metrics._
import io.renku.microservices.IOMicroservice
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentHashMap

object Microservice extends IOMicroservice {

  val ServicePort:             Int Refined Positive = 9005
  private implicit val logger: Logger[IO]           = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    sessionPoolResource <- new EventLogDbConfigProvider[IO]() map SessionPoolResource[IO, EventLogDB]
    exitCode            <- runMicroservice(sessionPoolResource)
  } yield exitCode

  private def runMicroservice(sessionPoolResource: Resource[IO, SessionResource[IO, EventLogDB]]) =
    sessionPoolResource.use { sessionResource =>
      for {
        certificateLoader           <- CertificateLoader[IO]
        sentryInitializer           <- SentryInitializer[IO]
        dbInitializer               <- DbInitializer(sessionResource)
        metricsRegistry             <- MetricsRegistry[IO]()
        queriesExecTimes            <- QueriesExecutionTimes(metricsRegistry)
        statsFinder                 <- StatsFinder(sessionResource, queriesExecTimes)
        eventLogMetrics             <- EventLogMetrics(metricsRegistry, statsFinder)
        awaitingGenerationGauge     <- AwaitingGenerationGauge(metricsRegistry, statsFinder)
        awaitingTransformationGauge <- AwaitingTransformationGauge(metricsRegistry, statsFinder)
        underTransformationGauge    <- UnderTransformationGauge(metricsRegistry, statsFinder)
        underTriplesGenerationGauge <- UnderTriplesGenerationGauge(metricsRegistry, statsFinder)
        metricsResetScheduler <- GaugeResetScheduler[IO, projects.Path](
                                   List(awaitingGenerationGauge,
                                        underTriplesGenerationGauge,
                                        awaitingTransformationGauge,
                                        underTransformationGauge
                                   ),
                                   MetricsConfigProvider()
                                 )
        creationSubscription <-
          events.categories.creation.SubscriptionFactory(sessionResource, awaitingGenerationGauge, queriesExecTimes)
        zombieEventsSubscription <- events.categories.zombieevents.SubscriptionFactory(
                                      sessionResource,
                                      awaitingGenerationGauge,
                                      underTriplesGenerationGauge,
                                      awaitingTransformationGauge,
                                      underTransformationGauge,
                                      queriesExecTimes
                                    )
        commitSyncRequestSubscription <- events.categories.commitsyncrequest.SubscriptionFactory(
                                           sessionResource,
                                           queriesExecTimes
                                         )
        statusChangeEventSubscription <- events.categories.statuschange.SubscriptionFactory(
                                           sessionResource,
                                           awaitingGenerationGauge,
                                           underTriplesGenerationGauge,
                                           awaitingTransformationGauge,
                                           underTransformationGauge,
                                           queriesExecTimes
                                         )
        eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                    creationSubscription,
                                    zombieEventsSubscription,
                                    commitSyncRequestSubscription,
                                    statusChangeEventSubscription
                                  )
        eventEndpoint            <- EventEndpoint(eventConsumersRegistry)
        processingStatusEndpoint <- ProcessingStatusEndpoint(sessionResource, queriesExecTimes)
        eventProducersRegistry <- EventProducersRegistry(
                                    sessionResource,
                                    awaitingGenerationGauge,
                                    underTriplesGenerationGauge,
                                    awaitingTransformationGauge,
                                    underTransformationGauge,
                                    queriesExecTimes
                                  )
        subscriptionsEndpoint <- SubscriptionsEndpoint(eventProducersRegistry)
        eventDetailsEndpoint  <- EventDetailsEndpoint(sessionResource, queriesExecTimes)
        eventsEndpoint        <- EventsEndpoint(sessionResource, queriesExecTimes)
        microserviceRoutes = new MicroserviceRoutes[IO](
                               eventEndpoint,
                               eventsEndpoint,
                               processingStatusEndpoint,
                               subscriptionsEndpoint,
                               eventDetailsEndpoint,
                               new RoutesMetrics[IO](metricsRegistry)
                             ).routes
        exitCode <- microserviceRoutes.use { routes =>
                      new MicroserviceRunner(
                        certificateLoader,
                        sentryInitializer,
                        dbInitializer,
                        eventLogMetrics,
                        eventProducersRegistry,
                        eventConsumersRegistry,
                        metricsResetScheduler,
                        HttpServer[IO](serverPort = ServicePort.value, routes),
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
    subProcessesCancelTokens: ConcurrentHashMap[IO[Unit], Unit]
) {

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

  private def gatherCancelToken(fiber: Fiber[IO, Throwable, Unit]): Fiber[IO, Throwable, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
