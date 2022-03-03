/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.effect.kernel.Ref
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.db.{SessionPoolResource, SessionResource}
import io.renku.eventlog.eventdetails.EventDetailsEndpoint
import io.renku.eventlog.events.categories.statuschange.StatusChangeEventsQueue
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
import io.renku.microservices.{IOMicroservice, ServiceReadinessChecker}
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger

object Microservice extends IOMicroservice {

  val ServicePort:             Int Refined Positive = 9005
  private implicit val logger: Logger[IO]           = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    sessionPoolResource <- new EventLogDbConfigProvider[IO]() map SessionPoolResource[IO, EventLogDB]
    exitCode            <- runMicroservice(sessionPoolResource)
  } yield exitCode

  private def runMicroservice(sessionPoolResource: Resource[IO, SessionResource[IO, EventLogDB]]) =
    sessionPoolResource.use { implicit sessionResource =>
      for {
        certificateLoader           <- CertificateLoader[IO]
        sentryInitializer           <- SentryInitializer[IO]
        isMigrating                 <- Ref.of[IO, Boolean](true)
        dbInitializer               <- DbInitializer[IO](isMigrating)
        metricsRegistry             <- MetricsRegistry[IO]()
        queriesExecTimes            <- QueriesExecutionTimes(metricsRegistry)
        eventsQueue                 <- StatusChangeEventsQueue[IO](sessionResource, queriesExecTimes)
        statsFinder                 <- StatsFinder(sessionResource, queriesExecTimes)
        eventLogMetrics             <- EventLogMetrics(metricsRegistry, statsFinder)
        awaitingGenerationGauge     <- AwaitingGenerationGauge(metricsRegistry, statsFinder)
        awaitingTransformationGauge <- AwaitingTransformationGauge(metricsRegistry, statsFinder)
        underTransformationGauge    <- UnderTransformationGauge(metricsRegistry, statsFinder)
        underTriplesGenerationGauge <- UnderTriplesGenerationGauge(metricsRegistry, statsFinder)
        awaitingDeletionGauge       <- AwaitingDeletionGauge(metricsRegistry, statsFinder)
        deletingGauge               <- DeletingGauge(metricsRegistry, statsFinder)
        metricsResetScheduler <- GaugeResetScheduler[IO, projects.Path](
                                   List(awaitingGenerationGauge,
                                        underTriplesGenerationGauge,
                                        awaitingTransformationGauge,
                                        underTransformationGauge,
                                        awaitingDeletionGauge,
                                        deletingGauge
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
        globalCommitSyncRequestSubscription <- events.categories.globalcommitsyncrequest.SubscriptionFactory(
                                                 sessionResource,
                                                 queriesExecTimes
                                               )
        statusChangeEventSubscription <- events.categories.statuschange.SubscriptionFactory(
                                           sessionResource,
                                           eventsQueue,
                                           awaitingGenerationGauge,
                                           underTriplesGenerationGauge,
                                           awaitingTransformationGauge,
                                           underTransformationGauge,
                                           awaitingDeletionGauge,
                                           deletingGauge,
                                           queriesExecTimes
                                         )
        eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                    creationSubscription,
                                    zombieEventsSubscription,
                                    commitSyncRequestSubscription,
                                    statusChangeEventSubscription,
                                    globalCommitSyncRequestSubscription
                                  )
        serviceReadinessChecker  <- ServiceReadinessChecker[IO](ServicePort)
        eventEndpoint            <- EventEndpoint(eventConsumersRegistry)
        processingStatusEndpoint <- ProcessingStatusEndpoint(sessionResource, queriesExecTimes)
        eventProducersRegistry <- EventProducersRegistry(
                                    sessionResource,
                                    awaitingGenerationGauge,
                                    underTriplesGenerationGauge,
                                    awaitingTransformationGauge,
                                    underTransformationGauge,
                                    awaitingDeletionGauge,
                                    deletingGauge,
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
                               new RoutesMetrics[IO](metricsRegistry),
                               isMigrating
                             ).routes
        exitCode <- microserviceRoutes.use { routes =>
                      new MicroserviceRunner(serviceReadinessChecker,
                                             certificateLoader,
                                             sentryInitializer,
                                             dbInitializer,
                                             eventLogMetrics,
                                             eventsQueue,
                                             eventProducersRegistry,
                                             eventConsumersRegistry,
                                             metricsResetScheduler,
                                             HttpServer[IO](serverPort = ServicePort.value, routes)
                      ).run()
                    }
      } yield exitCode
    }
}

private class MicroserviceRunner[F[_]: Spawn: Logger](
    serviceReadinessChecker: ServiceReadinessChecker[F],
    certificateLoader:       CertificateLoader[F],
    sentryInitializer:       SentryInitializer[F],
    dbInitializer:           DbInitializer[F],
    metrics:                 EventLogMetrics[F],
    eventsQueue:             StatusChangeEventsQueue[F],
    eventProducersRegistry:  EventProducersRegistry[F],
    eventConsumersRegistry:  EventConsumersRegistry[F],
    gaugeScheduler:          GaugeResetScheduler[F],
    httpServer:              HttpServer[F]
) {

  def run(): F[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- Spawn[F].start(dbInitializer.run() >> startDBDependentProcesses())
    result <- httpServer.run()
  } yield result

  private def startDBDependentProcesses() = for {
    _ <- Spawn[F].start(metrics.run())
    _ <- serviceReadinessChecker.waitIfNotUp
    _ <- Spawn[F].start(eventProducersRegistry.run())
    _ <- Spawn[F].start(eventConsumersRegistry.run())
    _ <- Spawn[F].start(eventsQueue.run())
    _ <- gaugeScheduler.run()
  } yield ()
}
