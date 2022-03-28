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
import io.renku.eventlog.events.categories.statuschange.StatusChangeEventsQueue
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics._
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
      MetricsRegistry[IO]().flatMap { implicit metricsRegistry =>
        for {
          certificateLoader           <- CertificateLoader[IO]
          sentryInitializer           <- SentryInitializer[IO]
          isMigrating                 <- Ref.of[IO, Boolean](true)
          dbInitializer               <- DbInitializer[IO](isMigrating)
          queriesExecTimes            <- QueriesExecutionTimes[IO]
          eventsQueue                 <- StatusChangeEventsQueue[IO](queriesExecTimes)
          statsFinder                 <- StatsFinder(queriesExecTimes)
          eventLogMetrics             <- EventLogMetrics(statsFinder)
          awaitingGenerationGauge     <- AwaitingGenerationGauge(statsFinder)
          awaitingTransformationGauge <- AwaitingTransformationGauge(statsFinder)
          underTransformationGauge    <- UnderTransformationGauge(statsFinder)
          underTriplesGenerationGauge <- UnderTriplesGenerationGauge(statsFinder)
          awaitingDeletionGauge       <- AwaitingDeletionGauge(statsFinder)
          deletingGauge               <- DeletingGauge(statsFinder)
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
            events.categories.creation.SubscriptionFactory(awaitingGenerationGauge, queriesExecTimes)
          zombieEventsSubscription <- events.categories.zombieevents.SubscriptionFactory(
                                        awaitingGenerationGauge,
                                        underTriplesGenerationGauge,
                                        awaitingTransformationGauge,
                                        underTransformationGauge,
                                        queriesExecTimes
                                      )
          commitSyncRequestSubscription <- events.categories.commitsyncrequest.SubscriptionFactory(queriesExecTimes)
          globalCommitSyncRequestSubscription <-
            events.categories.globalcommitsyncrequest.SubscriptionFactory(queriesExecTimes)
          statusChangeEventSubscription <- events.categories.statuschange.SubscriptionFactory(
                                             eventsQueue,
                                             awaitingGenerationGauge,
                                             underTriplesGenerationGauge,
                                             awaitingTransformationGauge,
                                             underTransformationGauge,
                                             awaitingDeletionGauge,
                                             deletingGauge,
                                             queriesExecTimes
                                           )
          migrationStatusChange <- events.categories.migrationstatuschange.SubscriptionFactory[IO](queriesExecTimes)
          eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                      creationSubscription,
                                      zombieEventsSubscription,
                                      commitSyncRequestSubscription,
                                      statusChangeEventSubscription,
                                      globalCommitSyncRequestSubscription,
                                      migrationStatusChange
                                    )
          serviceReadinessChecker <- ServiceReadinessChecker[IO](ServicePort)
          eventProducersRegistry <- EventProducersRegistry(
                                      awaitingGenerationGauge,
                                      underTriplesGenerationGauge,
                                      awaitingTransformationGauge,
                                      underTransformationGauge,
                                      awaitingDeletionGauge,
                                      deletingGauge,
                                      queriesExecTimes
                                    )
          microserviceRoutes <- MicroserviceRoutes[IO](
                                  eventConsumersRegistry,
                                  queriesExecTimes,
                                  eventProducersRegistry,
                                  isMigrating
                                ).map(_.routes)
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
