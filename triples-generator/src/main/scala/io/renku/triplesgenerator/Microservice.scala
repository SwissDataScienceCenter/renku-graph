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
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.microservices.IOMicroservice
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.config.certificates.GitCertificateInstaller
import io.renku.triplesgenerator.config.{IOVersionCompatibilityConfig, TriplesGeneration}
import io.renku.triplesgenerator.events.IOEventEndpoint
import io.renku.triplesgenerator.init.{CliVersionCompatibilityVerifier, IOCliVersionCompatibilityChecker}
import io.renku.triplesgenerator.reprovisioning.{IOReProvisioning, ReProvisioning, ReProvisioningStatus}
import org.typelevel.log4cats.Logger
import pureconfig._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext
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

  override def run(args: List[String]): IO[ExitCode] = for {
    config                         <- parseConfigArgs(args)
    certificateLoader              <- CertificateLoader[IO]
    gitCertificateInstaller        <- GitCertificateInstaller[IO]
    triplesGeneration              <- TriplesGeneration[IO](config)
    sentryInitializer              <- SentryInitializer[IO]
    metricsRegistry                <- MetricsRegistry()
    renkuVersionPairs              <- IOVersionCompatibilityConfig(ApplicationLogger, config)
    cliVersionCompatChecker        <- IOCliVersionCompatibilityChecker(triplesGeneration, renkuVersionPairs)
    gitLabRateLimit                <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler                <- Throttler[IO, GitLab](gitLabRateLimit)
    sparqlTimeRecorder             <- SparqlQueryTimeRecorder(metricsRegistry)
    awaitingGenerationSubscription <- events.categories.awaitinggeneration.SubscriptionFactory(metricsRegistry)
    membersSyncSubscription <- events.categories.membersync.SubscriptionFactory(gitLabThrottler, sparqlTimeRecorder)
    triplesGeneratedSubscription <-
      events.categories.triplesgenerated.SubscriptionFactory(metricsRegistry, gitLabThrottler, sparqlTimeRecorder)
    eventConsumersRegistry <- consumers.EventConsumersRegistry(
                                awaitingGenerationSubscription,
                                membersSyncSubscription,
                                triplesGeneratedSubscription
                              )
    reProvisioningStatus <- ReProvisioningStatus(eventConsumersRegistry, ApplicationLogger, sparqlTimeRecorder)
    reProvisioning <- IOReProvisioning(reProvisioningStatus, renkuVersionPairs, sparqlTimeRecorder, ApplicationLogger)
    eventProcessingEndpoint <- IOEventEndpoint(eventConsumersRegistry, reProvisioningStatus)
    microserviceRoutes =
      new MicroserviceRoutes[IO](eventProcessingEndpoint, new RoutesMetrics[IO](metricsRegistry), config.some).routes
    exitCode <- microserviceRoutes.use { routes =>
                  new MicroserviceRunner(
                    certificateLoader,
                    gitCertificateInstaller,
                    sentryInitializer,
                    cliVersionCompatChecker,
                    eventConsumersRegistry,
                    reProvisioning,
                    new HttpServer[IO](serverPort = ServicePort.value, routes),
                    subProcessesCancelTokens,
                    ApplicationLogger
                  ).run()
                }
  } yield exitCode
}

private class MicroserviceRunner(
    certificateLoader:               CertificateLoader[IO],
    gitCertificateInstaller:         GitCertificateInstaller[IO],
    sentryInitializer:               SentryInitializer[IO],
    cliVersionCompatibilityVerifier: CliVersionCompatibilityVerifier[IO],
    eventConsumersRegistry:          EventConsumersRegistry[IO],
    reProvisioning:                  ReProvisioning[IO],
    httpServer:                      HttpServer[IO],
    subProcessesCancelTokens:        ConcurrentHashMap[CancelToken[IO], Unit],
    logger:                          Logger[IO]
)(implicit contextShift:             ContextShift[IO]) {

  def run(): IO[ExitCode] = {
    for {
      _        <- certificateLoader.run()
      _        <- gitCertificateInstaller.run()
      _        <- sentryInitializer.run()
      _        <- cliVersionCompatibilityVerifier.run()
      _        <- eventConsumersRegistry.run().start.map(gatherCancelToken)
      _        <- reProvisioning.run().start.map(gatherCancelToken)
      exitCode <- httpServer.run()
    } yield exitCode
  } recoverWith logAndThrow(logger)

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }

  private def logAndThrow(logger: Logger[IO]): PartialFunction[Throwable, IO[ExitCode]] = { case NonFatal(exception) =>
    logger.error(exception)(exception.getMessage).flatMap(_ => exception.raiseError[IO, ExitCode])
  }
}
