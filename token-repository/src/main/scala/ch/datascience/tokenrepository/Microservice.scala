/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.db.DbTransactorResource
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.microservices.IOMicroservice
import ch.datascience.tokenrepository.repository.association.IOAssociateTokenEndpoint
import ch.datascience.tokenrepository.repository.deletion.IODeleteTokenEndpoint
import ch.datascience.tokenrepository.repository.fetching.IOFetchTokenEndpoint
import ch.datascience.tokenrepository.repository.init.IODbInitializer
import ch.datascience.tokenrepository.repository.{ProjectsTokensDB, ProjectsTokensDbConfigProvider}

import scala.language.higherKinds

object Microservice extends IOMicroservice {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new ProjectsTokensDbConfigProvider[IO] map DbTransactorResource[IO, ProjectsTokensDB]
      exitCode           <- runMicroservice(transactorResource, args)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, ProjectsTokensDB], args: List[String]) =
    transactorResource.use { transactor =>
      for {
        sentryInitializer      <- SentryInitializer[IO]
        fetchTokenEndpoint     <- IOFetchTokenEndpoint(transactor, ApplicationLogger)
        associateTokenEndpoint <- IOAssociateTokenEndpoint(transactor, ApplicationLogger)
        httpServer = new HttpServer[IO](
          serverPort = 9003,
          serviceRoutes = new MicroserviceRoutes[IO](
            fetchTokenEndpoint,
            associateTokenEndpoint,
            new IODeleteTokenEndpoint(transactor, ApplicationLogger)
          ).routes
        )

        exitCode <- new MicroserviceRunner(
                     sentryInitializer,
                     new IODbInitializer(transactor),
                     httpServer
                   ) run args
      } yield exitCode
    }
}

private class MicroserviceRunner(
    sentryInitializer: SentryInitializer[IO],
    dbInitializer:     IODbInitializer,
    httpServer:        HttpServer[IO]
) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _      <- sentryInitializer.run
      _      <- dbInitializer.run
      result <- httpServer.run
    } yield result
}
