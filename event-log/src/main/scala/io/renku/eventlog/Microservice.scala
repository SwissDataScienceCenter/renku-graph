/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import com.comcast.ip4s._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.db.{SessionPoolResource, SessionResource}
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEventsQueue
import io.renku.eventlog.events.producers.EventProducersRegistry
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics._
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.http.server.HttpServer
import io.renku.logging.ApplicationLogger
import io.renku.metrics._
import io.renku.microservices.{IOMicroservice, ServiceReadinessChecker}
import natchez.Trace.Implicits.noop
import org.http4s.server.Server
import org.typelevel.log4cats.Logger

object Microservice extends IOMicroservice {

  val ServicePort:             Port       = port"9005"
  private implicit val logger: Logger[IO] = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    sessionPoolResource <- new EventLogDbConfigProvider[IO]() map SessionPoolResource[IO, EventLogDB]
    exitCode            <- runMicroservice(sessionPoolResource)
  } yield exitCode

  private def runMicroservice(sessionPoolResource: Resource[IO, SessionResource[IO, EventLogDB]]) =
    sessionPoolResource.use { implicit sessionResource =>
      for {
        implicit0(mr: MetricsRegistry[IO])                  <- MetricsRegistry[IO]()
        implicit0(gc: GitLabClient[IO])                     <- GitLabClient[IO]()
        implicit0(acf: AccessTokenFinder[IO])               <- AccessTokenFinder[IO]()
        implicit0(qet: QueriesExecutionTimes[IO])           <- QueriesExecutionTimes[IO]()
        certificateLoader                                   <- CertificateLoader[IO]
        sentryInitializer                                   <- SentryInitializer[IO]
        isMigrating                                         <- Ref.of[IO, Boolean](true)
        dbInitializer                                       <- DbInitializer[IO](isMigrating)
        eventsQueue                                         <- StatusChangeEventsQueue[IO]
        statsFinder                                         <- StatsFinder[IO]
        eventLogMetrics                                     <- EventLogMetrics(statsFinder)
        implicit0(eventStatusGauges: EventStatusGauges[IO]) <- EventStatusGauges[IO](statsFinder)
        metricsResetScheduler <-
          GaugeResetScheduler[IO, projects.Path](eventStatusGauges.asList, MetricsConfigProvider())
        creationSubscription            <- events.consumers.creation.SubscriptionFactory[IO]
        zombieEventsSubscription        <- events.consumers.zombieevents.SubscriptionFactory[IO]
        commitSyncRequestSubscription   <- events.consumers.commitsyncrequest.SubscriptionFactory[IO]
        statusChangeEventSubscription   <- events.consumers.statuschange.SubscriptionFactory[IO](eventsQueue)
        globalCommitSyncReqSubscription <- events.consumers.globalcommitsyncrequest.SubscriptionFactory[IO]
        projectSyncSubscription         <- events.consumers.projectsync.SubscriptionFactory[IO]
        cleanUpRequestSubscription      <- events.consumers.cleanuprequest.SubscriptionFactory[IO]
        migrationStatusChange           <- events.consumers.migrationstatuschange.SubscriptionFactory[IO]
        eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                    creationSubscription,
                                    zombieEventsSubscription,
                                    commitSyncRequestSubscription,
                                    statusChangeEventSubscription,
                                    globalCommitSyncReqSubscription,
                                    projectSyncSubscription,
                                    cleanUpRequestSubscription,
                                    migrationStatusChange
                                  )
        serviceReadinessChecker <- ServiceReadinessChecker[IO](ServicePort)
        eventProducersRegistry  <- EventProducersRegistry[IO]
        microserviceRoutes <- MicroserviceRoutes[IO](eventConsumersRegistry, eventProducersRegistry, isMigrating)
                                .map(_.routes)
        exitCode <- microserviceRoutes.use { routes =>
                      new MicroserviceRunner(
                        serviceReadinessChecker,
                        certificateLoader,
                        sentryInitializer,
                        dbInitializer,
                        eventLogMetrics,
                        eventsQueue,
                        eventProducersRegistry,
                        eventConsumersRegistry,
                        metricsResetScheduler,
                        HttpServer[IO](serverPort = ServicePort, routes)
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

  def run(): F[ExitCode] =
    createServer.use(_ => Spawn[F].never[ExitCode])

  def createServer: Resource[F, Server] =
    for {
      _      <- Resource.eval(certificateLoader.run)
      _      <- Resource.eval(sentryInitializer.run)
      _      <- Resource.eval(Spawn[F].start(dbInitializer.run >> startDBDependentProcesses()))
      result <- httpServer.createServer
    } yield result

  private def startDBDependentProcesses() = for {
    _ <- Spawn[F].start(metrics.run)
    _ <- serviceReadinessChecker.waitIfNotUp
    _ <- Spawn[F].start(eventProducersRegistry.run)
    _ <- Spawn[F].start(eventConsumersRegistry.run)
    _ <- Spawn[F].start(eventsQueue.run)
    _ <- gaugeScheduler.run
  } yield ()
}
