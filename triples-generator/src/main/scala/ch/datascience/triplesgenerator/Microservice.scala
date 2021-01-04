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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool
import cats.effect._
import ch.datascience.config.GitLab
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.config.certificates.GitCertificateInstaller
import ch.datascience.triplesgenerator.eventprocessing._
import ch.datascience.triplesgenerator.init._
import ch.datascience.triplesgenerator.reprovisioning.{IOReProvisioning, ReProvisioning, ReProvisioningStatus}
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import pureconfig._

import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

  val ServicePort: Int Refined Positive = 9002

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      certificateLoader        <- CertificateLoader[IO](ApplicationLogger)
      gitCertificateInstaller  <- GitCertificateInstaller[IO](ApplicationLogger)
      sentryInitializer        <- SentryInitializer[IO]()
      fusekiDatasetInitializer <- IOFusekiDatasetInitializer()
      subscriber               <- Subscriber(ApplicationLogger)
      triplesGeneration        <- TriplesGeneration[IO]()
      metricsRegistry          <- MetricsRegistry()
      gitLabRateLimit          <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
      gitLabThrottler          <- Throttler[IO, GitLab](gitLabRateLimit)
      sparqlTimeRecorder       <- SparqlQueryTimeRecorder(metricsRegistry)
      reProvisioningStatus     <- ReProvisioningStatus(subscriber, ApplicationLogger, sparqlTimeRecorder)
      reProvisioning           <- IOReProvisioning(triplesGeneration, reProvisioningStatus, sparqlTimeRecorder, ApplicationLogger)
      eventProcessingEndpoint <- IOEventProcessingEndpoint(subscriber,
                                                           triplesGeneration,
                                                           reProvisioningStatus,
                                                           metricsRegistry,
                                                           gitLabThrottler,
                                                           sparqlTimeRecorder,
                                                           ApplicationLogger
                                 )
      microserviceRoutes =
        new MicroserviceRoutes[IO](eventProcessingEndpoint, new RoutesMetrics[IO](metricsRegistry)).routes
      exitCode <- microserviceRoutes.use { routes =>
                    new MicroserviceRunner(
                      certificateLoader,
                      gitCertificateInstaller,
                      sentryInitializer,
                      fusekiDatasetInitializer,
                      subscriber,
                      reProvisioning,
                      new HttpServer[IO](serverPort = ServicePort.value, routes),
                      subProcessesCancelTokens
                    ).run()
                  }
    } yield exitCode
}

private class MicroserviceRunner(
    certificateLoader:        CertificateLoader[IO],
    gitCertificateInstaller:  GitCertificateInstaller[IO],
    sentryInitializer:        SentryInitializer[IO],
    datasetInitializer:       FusekiDatasetInitializer[IO],
    subscriber:               Subscriber[IO],
    reProvisioning:           ReProvisioning[IO],
    httpServer:               HttpServer[IO],
    subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:      ContextShift[IO]) {

  def run(): IO[ExitCode] = for {
    _        <- certificateLoader.run()
    _        <- gitCertificateInstaller.run()
    _        <- sentryInitializer.run()
    _        <- datasetInitializer.run()
    _        <- subscriber.run().start.map(gatherCancelToken)
    _        <- reProvisioning.run().start.map(gatherCancelToken)
    exitCode <- httpServer.run()
  } yield exitCode

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
