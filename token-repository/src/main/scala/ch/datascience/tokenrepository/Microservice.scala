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

package ch.datascience.tokenrepository

import cats.effect._
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.db.{SessionPoolResource, SessionResource}
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import ch.datascience.tokenrepository.repository.association.IOAssociateTokenEndpoint
import ch.datascience.tokenrepository.repository.deletion.IODeleteTokenEndpoint
import ch.datascience.tokenrepository.repository.fetching.IOFetchTokenEndpoint
import ch.datascience.tokenrepository.repository.init.{DbInitializer, IODbInitializer}
import ch.datascience.tokenrepository.repository.metrics.QueriesExecutionTimes
import ch.datascience.tokenrepository.repository.{ProjectsTokensDB, ProjectsTokensDbConfigProvider}
import natchez.Trace.Implicits.noop

import scala.concurrent.ExecutionContext.Implicits.global

object Microservice extends IOMicroservice {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new ProjectsTokensDbConfigProvider[IO]() map SessionPoolResource[IO, ProjectsTokensDB]
      exitCode           <- runMicroservice(transactorResource, args)
    } yield exitCode

  private def runMicroservice(
      transactorResource: Resource[IO, SessionResource[IO, ProjectsTokensDB]],
      args:               List[String]
  ) = transactorResource.use { transactor =>
    for {
      certificateLoader      <- CertificateLoader[IO](ApplicationLogger)
      sentryInitializer      <- SentryInitializer[IO]()
      metricsRegistry        <- MetricsRegistry()
      queriesExecTimes       <- QueriesExecutionTimes(metricsRegistry)
      fetchTokenEndpoint     <- IOFetchTokenEndpoint(transactor, queriesExecTimes, ApplicationLogger)
      associateTokenEndpoint <- IOAssociateTokenEndpoint(transactor, queriesExecTimes, ApplicationLogger)
      dbInitializer          <- IODbInitializer(transactor, queriesExecTimes, ApplicationLogger)
      deleteTokenEndpoint    <- IODeleteTokenEndpoint(transactor, queriesExecTimes, ApplicationLogger)
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
