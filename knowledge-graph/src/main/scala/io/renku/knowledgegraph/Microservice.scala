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

package io.renku.knowledgegraph

import cats.effect._
import cats.syntax.all._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.http.server.HttpServer
import io.renku.knowledgegraph.metrics.KGMetrics
import io.renku.logging.ApplicationLogger
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.IOMicroservice
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

  protected implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  import cats.effect.unsafe.implicits.global

  private implicit val logger: Logger[IO] = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] =
    MetricsRegistry[IO]() >>= { implicit metricsRegistry =>
      SparqlQueryTimeRecorder[IO](metricsRegistry) >>= { implicit sparqlTimeRecorder =>
        for {
          certificateLoader  <- CertificateLoader[IO]
          sentryInitializer  <- SentryInitializer[IO]
          kgMetrics          <- KGMetrics[IO]
          microserviceRoutes <- MicroserviceRoutes()
          exicode <- microserviceRoutes.routes.use { routes =>
                       val httpServer = HttpServer[IO](serverPort = 9004, routes)
                       new MicroserviceRunner(certificateLoader, sentryInitializer, httpServer, kgMetrics).run()
                     }
        } yield exicode
      }
    }
}

private class MicroserviceRunner(
    certificateLoader: CertificateLoader[IO],
    sentryInitializer: SentryInitializer[IO],
    httpServer:        HttpServer[IO],
    kgMetrics:         KGMetrics[IO]
) {

  def run(): IO[ExitCode] = for {
    _        <- certificateLoader.run()
    _        <- sentryInitializer.run()
    _        <- kgMetrics.run().start
    exitCode <- httpServer.run()
  } yield exitCode
}
