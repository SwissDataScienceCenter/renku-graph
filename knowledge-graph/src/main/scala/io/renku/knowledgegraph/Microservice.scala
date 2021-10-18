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

package io.renku.knowledgegraph

import cats.effect._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.http.server.HttpServer
import io.renku.knowledgegraph.metrics.KGMetrics
import io.renku.logging.ApplicationLogger
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.IOMicroservice
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import pureconfig.ConfigSource

import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

  protected implicit override val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  private implicit val logger:                  Logger[IO]       = ApplicationLogger
  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)
  protected implicit override def timer:        Timer[IO]        = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] = for {
    certificateLoader  <- CertificateLoader[IO](logger)
    sentryInitializer  <- SentryInitializer[IO]()
    metricsRegistry    <- MetricsRegistry()
    sparqlTimeRecorder <- SparqlQueryTimeRecorder(metricsRegistry)
    kgMetrics          <- KGMetrics(metricsRegistry, sparqlTimeRecorder)
    microserviceRoutes <- MicroserviceRoutes(metricsRegistry, sparqlTimeRecorder, logger)
    exicode <- microserviceRoutes.routes.use { routes =>
                 val httpServer = new HttpServer[IO](serverPort = 9004, routes)
                 new MicroserviceRunner(certificateLoader, sentryInitializer, httpServer, kgMetrics).run()
               }
  } yield exicode
}

private class MicroserviceRunner(
    certificateLoader:   CertificateLoader[IO],
    sentryInitializer:   SentryInitializer[IO],
    httpServer:          HttpServer[IO],
    kgMetrics:           KGMetrics[IO]
)(implicit contextShift: ContextShift[IO]) {

  def run(): IO[ExitCode] = for {
    _        <- certificateLoader.run()
    _        <- sentryInitializer.run()
    _        <- kgMetrics.run().start
    exitCode <- httpServer.run()
  } yield exitCode
}
