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

package ch.datascience.graphservice

import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.graphservice.graphql.IOQueryEndpoint
import ch.datascience.http.server.HttpServer
import pureconfig.loadConfigOrThrow

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object Microservice extends IOApp {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(loadConfigOrThrow[Int]("threads-number"))

  protected implicit override def contextShift: ContextShift[IO] =
    IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] =
    IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] = runMicroservice(args)

  private def runMicroservice(args: List[String]) = httpServer flatMap (_.run)

  private lazy val httpServer = for {
    queryEndpoint <- IOQueryEndpoint()
  } yield
    new HttpServer[IO](
      serverPort    = 9004,
      serviceRoutes = new MicroserviceRoutes[IO](queryEndpoint).routes
    )
}
