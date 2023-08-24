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

package io.renku.triplesgenerator

import cats.effect._
import cats.syntax.all._
import com.comcast.ip4s._
import com.typesafe.config.{Config, ConfigFactory}
import fs2.concurrent.{Signal, SignallingRef}
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.db.{SessionPoolResource, SessionResource}
import io.renku.entities.viewings
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.http.server.HttpServer
import io.renku.logging.ApplicationLogger
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.{IOMicroservice, ResourceUse, ServiceReadinessChecker}
import io.renku.triplesgenerator.config.certificates.GitCertificateInstaller
import io.renku.triplesgenerator.events.consumers._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesgenerator.events.consumers.tsprovisioning.{minprojectinfo, triplesgenerated}
import io.renku.triplesgenerator.init.{CliVersionCompatibilityChecker, CliVersionCompatibilityVerifier}
import io.renku.triplesgenerator.metrics.MetricsService
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import natchez.Trace.Implicits.noop
import org.http4s.server.Server
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NonFatal

object Microservice extends IOMicroservice {

  val ServicePort:             Port       = port"9002"
  private implicit val logger: Logger[IO] = ApplicationLogger

  private def parseConfigArgs(args: List[String]): IO[Config] = IO {
    args.headOption match {
      case Some(configFileName) => ConfigFactory.load(configFileName)
      case None                 => ConfigFactory.load
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val resources = for {
      config <- Resource.eval(parseConfigArgs(args))
      dbSessionPool <- Resource
                         .eval(new TgLockDbConfigProvider[IO].map(SessionPoolResource[IO, TgLockDB]))
                         .flatMap(identity)
      projectConnConfig <- Resource.eval(ProjectsConnectionConfig[IO](config))
      projectsSparql    <- ProjectSparqlClient[IO](projectConnConfig)
    } yield (config, dbSessionPool, projectsSparql)

    resources.use { case (config, dbSessionPool, projectSparqlClient) =>
      doRun(config, dbSessionPool, projectSparqlClient)
    }
  }

  private def doRun(
      config:              Config,
      dbSessionPool:       SessionResource[IO, TgLockDB],
      projectSparqlClient: ProjectSparqlClient[IO]
  ): IO[ExitCode] = for {
    implicit0(mr: MetricsRegistry[IO])           <- MetricsRegistry[IO]()
    implicit0(sqtr: SparqlQueryTimeRecorder[IO]) <- SparqlQueryTimeRecorder[IO]()
    implicit0(gc: GitLabClient[IO])              <- GitLabClient[IO]()
    implicit0(acf: AccessTokenFinder[IO])        <- AccessTokenFinder[IO]()
    implicit0(rp: ReProvisioningStatus[IO])      <- ReProvisioningStatus[IO]()

    _ <- TgLockDB.migrate[IO](dbSessionPool, 20.seconds)

    metricsService <- MetricsService[IO](dbSessionPool)
    _ <- metricsService.collectEvery(Duration.fromNanos(config.getDuration("metrics-interval").toNanos)).start

    tsWriteLock = TgLockDB.createLock[IO, projects.Slug](dbSessionPool, 0.5.seconds)
    projectConnConfig              <- ProjectsConnectionConfig[IO](config)
    certificateLoader              <- CertificateLoader[IO]
    gitCertificateInstaller        <- GitCertificateInstaller[IO]
    sentryInitializer              <- SentryInitializer[IO]
    cliVersionCompatChecker        <- CliVersionCompatibilityChecker[IO](config)
    awaitingGenerationSubscription <- awaitinggeneration.SubscriptionFactory[IO]
    membersSyncSubscription        <- membersync.SubscriptionFactory[IO](tsWriteLock, projectSparqlClient)
    triplesGeneratedSubscription   <- triplesgenerated.SubscriptionFactory[IO](tsWriteLock)
    cleanUpSubscription            <- cleanup.SubscriptionFactory[IO](tsWriteLock)
    minProjectInfoSubscription     <- minprojectinfo.SubscriptionFactory[IO](tsWriteLock)
    migrationRequestSubscription   <- tsmigrationrequest.SubscriptionFactory[IO](config)
    syncRepoMetadataSubscription   <- syncrepometadata.SubscriptionFactory[IO](config, tsWriteLock)
    projectActivationsSubscription <- viewings.collector.projects.activated.SubscriptionFactory[IO](projectConnConfig)
    projectViewingsSubscription    <- viewings.collector.projects.viewed.SubscriptionFactory[IO](projectConnConfig)
    datasetViewingsSubscription    <- viewings.collector.datasets.SubscriptionFactory[IO](projectConnConfig)
    viewingDeletionSubscription    <- viewings.deletion.projects.SubscriptionFactory[IO](projectConnConfig)
    eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                awaitingGenerationSubscription,
                                membersSyncSubscription,
                                triplesGeneratedSubscription,
                                minProjectInfoSubscription,
                                cleanUpSubscription,
                                migrationRequestSubscription,
                                syncRepoMetadataSubscription,
                                projectActivationsSubscription,
                                projectViewingsSubscription,
                                datasetViewingsSubscription,
                                viewingDeletionSubscription
                              )
    serviceReadinessChecker <- ServiceReadinessChecker[IO](ServicePort)
    microserviceRoutes      <- MicroserviceRoutes[IO](eventConsumersRegistry, config).map(_.routes)
    termSignal              <- SignallingRef.of[IO, Boolean](false)
    exitCode <- microserviceRoutes.use { routes =>
                  new MicroserviceRunner[IO](
                    serviceReadinessChecker,
                    certificateLoader,
                    gitCertificateInstaller,
                    sentryInitializer,
                    cliVersionCompatChecker,
                    eventConsumersRegistry,
                    HttpServer[IO](serverPort = ServicePort, routes)
                  ).run(termSignal)
                }
  } yield exitCode
}

private class MicroserviceRunner[F[_]: Async: Logger](
    serviceReadinessChecker:         ServiceReadinessChecker[F],
    certificateLoader:               CertificateLoader[F],
    gitCertificateInstaller:         GitCertificateInstaller[F],
    sentryInitializer:               SentryInitializer[F],
    cliVersionCompatibilityVerifier: CliVersionCompatibilityVerifier[F],
    eventConsumersRegistry:          EventConsumersRegistry[F],
    httpServer:                      HttpServer[F]
) {

  def run(signal: Signal[F, Boolean]): F[ExitCode] =
    Ref.of[F, ExitCode](ExitCode.Success).flatMap(rc => ResourceUse(createServer).useUntil(signal, rc))

  private def createServer: Resource[F, Server] = {
    for {
      _      <- Resource.eval(certificateLoader.run)
      _      <- Resource.eval(gitCertificateInstaller.run)
      _      <- Resource.eval(sentryInitializer.run)
      _      <- Resource.eval(cliVersionCompatibilityVerifier.run)
      _      <- Spawn[F].background(serviceReadinessChecker.waitIfNotUp >> eventConsumersRegistry.run)
      server <- httpServer.createServer
      _      <- Resource.eval(Logger[F].info(s"Triples-Generator service started"))
    } yield server
  } recoverWith logAndThrow

  private lazy val logAndThrow: PartialFunction[Throwable, Resource[F, Server]] = { case NonFatal(exception) =>
    Resource.eval(Logger[F].error(exception)(exception.getMessage).flatMap(_ => exception.raiseError[F, Server]))
  }
}
