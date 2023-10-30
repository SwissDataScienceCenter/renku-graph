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

package io.renku.knowledgegraph

import cats.effect._
import com.comcast.ip4s._
import fs2.concurrent.{Signal, SignallingRef}
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.http.server.HttpServer
import io.renku.knowledgegraph.metrics.KGMetrics
import io.renku.logging.ApplicationLogger
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.{IOMicroservice, ResourceUse}
import io.renku.triplesstore.{ProjectSparqlClient, ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s.server.Server
import org.typelevel.log4cats.Logger

object Microservice extends IOMicroservice {

  private implicit val logger: Logger[IO] = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] =
    Setup.resource.use { setup =>
      implicit val sqtr: SparqlQueryTimeRecorder[IO] = setup.sparqlQueryTimeRecorder
      implicit val mr:   MetricsRegistry[IO]         = setup.metricsRegistry
      for {
        certificateLoader  <- CertificateLoader[IO]
        sentryInitializer  <- SentryInitializer[IO]
        kgMetrics          <- KGMetrics[IO]
        microserviceRoutes <- MicroserviceRoutes[IO](setup.projectConnConfig, setup.projectSparqlClient)
        termSignal         <- SignallingRef.of[IO, Boolean](false)
        exitCode <- microserviceRoutes.routes.use { routes =>
                      val httpServer = HttpServer[IO](serverPort = port"9004", routes)
                      new MicroserviceRunner(certificateLoader, sentryInitializer, httpServer, kgMetrics).run(
                        termSignal
                      )
                    }
      } yield exitCode
    }

  private final case class Setup(
      projectConnConfig:       ProjectsConnectionConfig,
      metricsRegistry:         MetricsRegistry[IO],
      sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[IO],
      projectSparqlClient:     ProjectSparqlClient[IO]
  )

  private object Setup {
    val resource: Resource[IO, Setup] = for {
      pcc <- Resource.eval(ProjectsConnectionConfig[IO]())

      implicit0(mr: MetricsRegistry[IO])           <- Resource.eval(MetricsRegistry[IO]())
      implicit0(sqtr: SparqlQueryTimeRecorder[IO]) <- Resource.eval(SparqlQueryTimeRecorder.create[IO]())
      psc                                          <- ProjectSparqlClient[IO](pcc)
    } yield Setup(pcc, mr, sqtr, psc)
  }
}

private class MicroserviceRunner(
    certificateLoader: CertificateLoader[IO],
    sentryInitializer: SentryInitializer[IO],
    httpServer:        HttpServer[IO],
    kgMetrics:         KGMetrics[IO]
) {

  def run(signal: Signal[IO, Boolean]): IO[ExitCode] =
    Ref.of[IO, ExitCode](ExitCode.Success).flatMap(rc => ResourceUse(createServer).useUntil(signal, rc))

  private def createServer: Resource[IO, Server] = for {
    _      <- Resource.eval(certificateLoader.run)
    _      <- Resource.eval(sentryInitializer.run)
    _      <- Resource.eval(kgMetrics.run.start)
    result <- httpServer.createServer
  } yield result
}
