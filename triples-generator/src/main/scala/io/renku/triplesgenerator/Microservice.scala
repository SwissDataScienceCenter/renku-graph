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

package io.renku.triplesgenerator

import cats.effect._
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.http.server.HttpServer
import io.renku.logging.ApplicationLogger
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.{IOMicroservice, ServiceReadinessChecker}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.config.certificates.GitCertificateInstaller
import io.renku.triplesgenerator.events.consumers._
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesgenerator.events.consumers.tsprovisioning.{minprojectinfo, triplesgenerated}
import io.renku.triplesgenerator.init.{CliVersionCompatibilityChecker, CliVersionCompatibilityVerifier}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

object Microservice extends IOMicroservice {

  val ServicePort:             Int Refined Positive = 9002
  private implicit val logger: Logger[IO]           = ApplicationLogger

  private def parseConfigArgs(args: List[String]): IO[Config] = IO {
    args.headOption match {
      case Some(configFileName) => ConfigFactory.load(configFileName)
      case None                 => ConfigFactory.load
    }
  }

  override def run(args: List[String]): IO[ExitCode] =
    MetricsRegistry[IO]() >>= { implicit metricsRegistry =>
      SparqlQueryTimeRecorder[IO](metricsRegistry) >>= { implicit sparqlTimeRecorder =>
        GitLabClient[IO]() >>= { implicit gitLabClient =>
          AccessTokenFinder[IO]() >>= { implicit accessTokenFinder =>
            for {
              config                         <- parseConfigArgs(args)
              certificateLoader              <- CertificateLoader[IO]
              gitCertificateInstaller        <- GitCertificateInstaller[IO]
              sentryInitializer              <- SentryInitializer[IO]
              cliVersionCompatChecker        <- CliVersionCompatibilityChecker[IO](config)
              awaitingGenerationSubscription <- awaitinggeneration.SubscriptionFactory[IO]
              membersSyncSubscription        <- membersync.SubscriptionFactory[IO]
              triplesGeneratedSubscription   <- triplesgenerated.SubscriptionFactory[IO]
              minProjectInfoSubscription     <- minprojectinfo.SubscriptionFactory[IO]
              cleanUpSubscription            <- cleanup.SubscriptionFactory[IO]
              reProvisioningStatus <- ReProvisioningStatus(
                                        awaitingGenerationSubscription,
                                        membersSyncSubscription,
                                        triplesGeneratedSubscription,
                                        minProjectInfoSubscription,
                                        cleanUpSubscription
                                      )
              migrationRequestSubscription <- tsmigrationrequest.SubscriptionFactory[IO](reProvisioningStatus, config)
              eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                          awaitingGenerationSubscription,
                                          membersSyncSubscription,
                                          triplesGeneratedSubscription,
                                          minProjectInfoSubscription,
                                          cleanUpSubscription,
                                          migrationRequestSubscription
                                        )
              serviceReadinessChecker <- ServiceReadinessChecker[IO](ServicePort)
              microserviceRoutes <-
                MicroserviceRoutes[IO](eventConsumersRegistry, reProvisioningStatus, config.some).map(_.routes)
              exitCode <- microserviceRoutes.use { routes =>
                            new MicroserviceRunner[IO](
                              serviceReadinessChecker,
                              certificateLoader,
                              gitCertificateInstaller,
                              sentryInitializer,
                              cliVersionCompatChecker,
                              eventConsumersRegistry,
                              HttpServer[IO](serverPort = ServicePort.value, routes)
                            ).run()
                          }
            } yield exitCode
          }
        }
      }
    }
}

private class MicroserviceRunner[F[_]: Spawn: Logger](
    serviceReadinessChecker:         ServiceReadinessChecker[F],
    certificateLoader:               CertificateLoader[F],
    gitCertificateInstaller:         GitCertificateInstaller[F],
    sentryInitializer:               SentryInitializer[F],
    cliVersionCompatibilityVerifier: CliVersionCompatibilityVerifier[F],
    eventConsumersRegistry:          EventConsumersRegistry[F],
    httpServer:                      HttpServer[F]
) {

  def run(): F[ExitCode] = {
    for {
      _        <- certificateLoader.run()
      _        <- gitCertificateInstaller.run()
      _        <- sentryInitializer.run()
      _        <- cliVersionCompatibilityVerifier.run()
      _        <- Spawn[F].start(serviceReadinessChecker.waitIfNotUp >> eventConsumersRegistry.run())
      exitCode <- httpServer.run()
    } yield exitCode
  } recoverWith logAndThrow

  private lazy val logAndThrow: PartialFunction[Throwable, F[ExitCode]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(exception.getMessage).flatMap(_ => exception.raiseError[F, ExitCode])
  }
}
