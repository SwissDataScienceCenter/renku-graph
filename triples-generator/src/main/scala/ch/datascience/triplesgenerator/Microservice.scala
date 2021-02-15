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

package ch.datascience.triplesgenerator

import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.SubscriptionsRegistry
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.config.certificates.GitCertificateInstaller
import ch.datascience.triplesgenerator.config.{IOVersionCompatibilityConfig, TriplesGeneration}
import ch.datascience.triplesgenerator.events.IOEventEndpoint
import ch.datascience.triplesgenerator.init._
import ch.datascience.triplesgenerator.reprovisioning.{IOReProvisioning, ReProvisioning, ReProvisioningStatus}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.chrisdavenport.log4cats.Logger
import pureconfig._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

object Microservice extends IOMicroservice {

  val ServicePort: Int Refined Positive = 9002

  protected implicit override val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      certificateLoader        <- CertificateLoader[IO](ApplicationLogger)
      gitCertificateInstaller  <- GitCertificateInstaller[IO](ApplicationLogger)
      fusekiDatasetInitializer <- IOFusekiDatasetInitializer()

      triplesGeneration       <- TriplesGeneration[IO]()
      sentryInitializer       <- SentryInitializer[IO]()
      metricsRegistry         <- MetricsRegistry()
      renkuVersionPairs       <- IOVersionCompatibilityConfig(ApplicationLogger)
      cliVersionCompatChecker <- IOCliVersionCompatibilityChecker(triplesGeneration, renkuVersionPairs)
      gitLabRateLimit         <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
      gitLabThrottler         <- Throttler[IO, GitLab](gitLabRateLimit)
      sparqlTimeRecorder      <- SparqlQueryTimeRecorder(metricsRegistry)
      awaitingGenerationSubscription <- events.categories.awaitinggeneration.SubscriptionFactory(renkuVersionPairs.head,
                                                                                                 metricsRegistry,
                                                                                                 gitLabThrottler,
                                                                                                 sparqlTimeRecorder,
                                                                                                 ApplicationLogger
                                        )

      membersSyncSubscription <-
        events.categories.membersync.SubscriptionFactory(gitLabThrottler, ApplicationLogger, sparqlTimeRecorder)
      triplesGeneratedSubscription <-
        events.categories.triplesgenerated.SubscriptionFactory(metricsRegistry,
                                                               gitLabThrottler,
                                                               sparqlTimeRecorder,
                                                               ApplicationLogger
        )
      subscriptionsRegistry <- consumers.SubscriptionsRegistry(ApplicationLogger,
                                                               awaitingGenerationSubscription,
                                                               membersSyncSubscription,
                                                               triplesGeneratedSubscription
                               )

      reProvisioningStatus <- ReProvisioningStatus(subscriptionsRegistry, ApplicationLogger, sparqlTimeRecorder)
      reProvisioning       <- IOReProvisioning(reProvisioningStatus, renkuVersionPairs, sparqlTimeRecorder, ApplicationLogger)
      eventProcessingEndpoint <- IOEventEndpoint(renkuVersionPairs.head,
                                                 metricsRegistry,
                                                 gitLabThrottler,
                                                 sparqlTimeRecorder,
                                                 subscriptionsRegistry,
                                                 reProvisioningStatus,
                                                 ApplicationLogger
                                 )
      microserviceRoutes =
        new MicroserviceRoutes[IO](eventProcessingEndpoint, new RoutesMetrics[IO](metricsRegistry)).routes
      exitCode <- microserviceRoutes.use { routes =>
                    new MicroserviceRunner(
                      certificateLoader,
                      gitCertificateInstaller,
                      sentryInitializer,
                      cliVersionCompatChecker,
                      fusekiDatasetInitializer,
                      subscriptionsRegistry,
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
    datasetInitializer:              FusekiDatasetInitializer[IO],
    subscriptionsRegistry:           SubscriptionsRegistry[IO],
    reProvisioning:                  ReProvisioning[IO],
    httpServer:                      HttpServer[IO],
    subProcessesCancelTokens:        ConcurrentHashMap[CancelToken[IO], Unit],
    logger:                          Logger[IO]
)(implicit contextShift:             ContextShift[IO]) {

  def run(): IO[ExitCode] = (for {
    _        <- certificateLoader.run()
    _        <- gitCertificateInstaller.run()
    _        <- sentryInitializer.run()
    _        <- cliVersionCompatibilityVerifier.run()
    _        <- datasetInitializer.run()
    _        <- subscriptionsRegistry.run().start.map(gatherCancelToken)
    _        <- reProvisioning.run().start.map(gatherCancelToken)
    exitCode <- httpServer.run()
  } yield exitCode) recoverWith logAndThrow(logger)

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }

  private def logAndThrow(logger: Logger[IO]): PartialFunction[Throwable, IO[ExitCode]] = { case NonFatal(exception) =>
    logger.error(exception)(exception.getMessage).flatMap(_ => exception.raiseError[IO, ExitCode])
  }
}
