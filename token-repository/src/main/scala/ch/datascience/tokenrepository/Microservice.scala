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
import ch.datascience.http.server.HttpServer
import ch.datascience.tokenrepository.repository.ProjectsTokensDbConfigProvider
import ch.datascience.tokenrepository.repository.association.IOAssociateTokenEndpoint
import ch.datascience.tokenrepository.repository.deletion.IODeleteTokenEndpoint
import ch.datascience.tokenrepository.repository.fetching.IOFetchTokenEndpoint
import ch.datascience.tokenrepository.repository.init.IODbInitializer

import scala.language.higherKinds

object Microservice extends IOApp {

  private val microserviceInstantiator = for {
    dbConfig <- new ProjectsTokensDbConfigProvider[IO].get()
    dbInitializer = new IODbInitializer(dbConfig)

    httpServer = new HttpServer[IO](
      serverPort = 9003,
      serviceRoutes = new MicroserviceRoutes[IO](
        new IOFetchTokenEndpoint(dbConfig),
        new IOAssociateTokenEndpoint(dbConfig),
        new IODeleteTokenEndpoint(dbConfig)
      ).routes
    )
  } yield new MicroserviceRunner(dbInitializer, httpServer)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      microservice <- microserviceInstantiator
      exitCode     <- microservice.run(args)
    } yield exitCode
}

class MicroserviceRunner(
    dbInitializer: IODbInitializer,
    httpServer:    HttpServer[IO]
) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _      <- dbInitializer.run
      result <- httpServer.run
    } yield result
}
