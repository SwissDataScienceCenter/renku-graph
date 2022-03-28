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
import io.renku.config.GitLab
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.control.{RateLimit, Throttler}
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.http.server.HttpServer
import io.renku.logging.ApplicationLogger
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.{IOMicroservice, ServiceReadinessChecker}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.config.certificates.GitCertificateInstaller
import io.renku.triplesgenerator.config.{TriplesGeneration, VersionCompatibilityConfig}
import io.renku.triplesgenerator.init.{CliVersionCompatibilityChecker, CliVersionCompatibilityVerifier}
import io.renku.triplesgenerator.reprovisioning.{ReProvisioning, ReProvisioningStatus}
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
    MetricsRegistry[IO]() flatMap { implicit metricsRegistry =>
      for {
        config                         <- parseConfigArgs(args)
        certificateLoader              <- CertificateLoader[IO]
        gitCertificateInstaller        <- GitCertificateInstaller[IO]
        triplesGeneration              <- TriplesGeneration[IO](config)
        sentryInitializer              <- SentryInitializer[IO]
        renkuVersionPairs              <- VersionCompatibilityConfig[IO](config)
        cliVersionCompatChecker        <- CliVersionCompatibilityChecker[IO](triplesGeneration, renkuVersionPairs)
        gitLabRateLimit                <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
        gitLabThrottler                <- Throttler[IO, GitLab](gitLabRateLimit)
        sparqlTimeRecorder             <- SparqlQueryTimeRecorder[IO]
        awaitingGenerationSubscription <- events.categories.awaitinggeneration.SubscriptionFactory[IO]
        membersSyncSubscription <- events.categories.membersync.SubscriptionFactory(gitLabThrottler, sparqlTimeRecorder)
        triplesGeneratedSubscription <-
          events.categories.triplesgenerated.SubscriptionFactory(gitLabThrottler, sparqlTimeRecorder)
        cleanUpSubscription          <- events.categories.cleanup.SubscriptionFactory(sparqlTimeRecorder)
        migrationRequestSubscription <- events.categories.tsmigrationrequest.SubscriptionFactory[IO]
        eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                    awaitingGenerationSubscription,
                                    membersSyncSubscription,
                                    triplesGeneratedSubscription,
                                    cleanUpSubscription,
                                    migrationRequestSubscription
                                  )
        reProvisioningStatus    <- ReProvisioningStatus(eventConsumersRegistry, sparqlTimeRecorder)
        reProvisioning          <- ReProvisioning(reProvisioningStatus, renkuVersionPairs, sparqlTimeRecorder)
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
                        reProvisioning,
                        HttpServer[IO](serverPort = ServicePort.value, routes)
                      ).run()
                    }
      } yield exitCode
    }
}

private class MicroserviceRunner[F[_]: Spawn: Logger](
    serviceReadinessChecker:         ServiceReadinessChecker[F],
    certificateLoader:               CertificateLoader[F],
    gitCertificateInstaller:         GitCertificateInstaller[F],
    sentryInitializer:               SentryInitializer[F],
    cliVersionCompatibilityVerifier: CliVersionCompatibilityVerifier[F],
    eventConsumersRegistry:          EventConsumersRegistry[F],
    reProvisioning:                  ReProvisioning[F],
    httpServer:                      HttpServer[F]
) {

  def run(): F[ExitCode] = {
    for {
      _ <- certificateLoader.run()
      _ <- gitCertificateInstaller.run()
      _ <- sentryInitializer.run()
      _ <- cliVersionCompatibilityVerifier.run()
      _ <- Spawn[F].start(serviceReadinessChecker.waitIfNotUp >> reProvisioning.run() >> eventConsumersRegistry.run())
      exitCode <- httpServer.run()
    } yield exitCode
  } recoverWith logAndThrow

  private lazy val logAndThrow: PartialFunction[Throwable, F[ExitCode]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(exception.getMessage).flatMap(_ => exception.raiseError[F, ExitCode])
  }
}
