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

package io.renku.tokenrepository

import cats.effect._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.db.{SessionPoolResource, SessionResource}
import io.renku.http.server.HttpServer
import io.renku.logging.ApplicationLogger
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.microservices.IOMicroservice
import io.renku.tokenrepository.repository.association.IOAssociateTokenEndpoint
import io.renku.tokenrepository.repository.deletion.IODeleteTokenEndpoint
import io.renku.tokenrepository.repository.fetching.IOFetchTokenEndpoint
import io.renku.tokenrepository.repository.init.{DbInitializer, IODbInitializer}
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import io.renku.tokenrepository.repository.{ProjectsTokensDB, ProjectsTokensDbConfigProvider}
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext.Implicits.global

object Microservice extends IOMicroservice {

  private implicit val logger: Logger[IO] = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    sessionPoolResource <- new ProjectsTokensDbConfigProvider[IO]() map SessionPoolResource[IO, ProjectsTokensDB]
    exitCode            <- runMicroservice(sessionPoolResource)
  } yield exitCode

  private def runMicroservice(
      sessionPoolResource: Resource[IO, SessionResource[IO, ProjectsTokensDB]]
  ) = sessionPoolResource.use { sessionResource =>
    for {
      certificateLoader      <- CertificateLoader[IO](logger)
      sentryInitializer      <- SentryInitializer[IO]()
      metricsRegistry        <- MetricsRegistry()
      queriesExecTimes       <- QueriesExecutionTimes(metricsRegistry)
      fetchTokenEndpoint     <- IOFetchTokenEndpoint(sessionResource, queriesExecTimes)
      associateTokenEndpoint <- IOAssociateTokenEndpoint(sessionResource, queriesExecTimes, logger)
      dbInitializer          <- IODbInitializer(sessionResource, queriesExecTimes, logger)
      deleteTokenEndpoint    <- IODeleteTokenEndpoint(sessionResource, queriesExecTimes, logger)
      microserviceRoutes = new MicroserviceRoutes[IO](
                             fetchTokenEndpoint,
                             associateTokenEndpoint,
                             deleteTokenEndpoint,
                             new RoutesMetrics[IO](metricsRegistry)
                           ).routes
      exitcode <- microserviceRoutes.use { routes =>
                    val httpServer = new HttpServer[IO](serverPort = 9003, routes)

                    new MicroserviceRunner(
                      certificateLoader,
                      sentryInitializer,
                      dbInitializer,
                      httpServer
                    ).run()
                  }
    } yield exitcode
  }
}

private class MicroserviceRunner(
    certificateLoader: CertificateLoader[IO],
    sentryInitializer: SentryInitializer[IO],
    dbInitializer:     DbInitializer[IO],
    httpServer:        HttpServer[IO]
) {

  def run(): IO[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- dbInitializer.run()
    result <- httpServer.run()
  } yield result
}
