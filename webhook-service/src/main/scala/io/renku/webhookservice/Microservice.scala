/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice

import cats.effect._
import fs2.concurrent.{Signal, SignallingRef}
import io.renku.config.certificates.CertificateLoader
import io.renku.http.client.{GitLabClient, GitLabClientLoader}
import io.renku.http.server.HttpServer
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder, ExecutionTimeRecorderLoader}
import io.renku.metrics.{MetricsRegistry, MetricsRegistryLoader}
import io.renku.microservices.IOMicroservice
import com.comcast.ip4s._
import io.renku.config.sentry.SentryInitializer
import io.renku.utils.common.ResourceUse
import org.http4s.server.Server

object Microservice extends IOMicroservice {

  private implicit val logger: ApplicationLogger.type = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    implicit0(mr: MetricsRegistry[IO])        <- MetricsRegistryLoader[IO]()
    implicit0(gc: GitLabClient[IO])           <- GitLabClientLoader[IO]()
    implicit0(etr: ExecutionTimeRecorder[IO]) <- ExecutionTimeRecorderLoader[IO]()
    certificateLoader                         <- CertificateLoader[IO]
    sentryInitializer                         <- SentryInitializer[IO]
    microserviceRoutes                        <- MicroserviceRoutes[IO]
    termSignal                                <- SignallingRef.of[IO, Boolean](false)
    exitCode <- microserviceRoutes.routes.use { routes =>
                  val httpServer = HttpServer[IO](port"9001", routes)
                  new MicroserviceRunner(certificateLoader, sentryInitializer, httpServer).run(termSignal)
                }
  } yield exitCode
}

class MicroserviceRunner(certificateLoader: CertificateLoader[IO],
                         sentryInitializer: SentryInitializer[IO],
                         httpServer:        HttpServer[IO]
) {

  def run(signal: Signal[IO, Boolean]): IO[ExitCode] =
    Ref.of[IO, ExitCode](ExitCode.Success).flatMap(rc => ResourceUse(createServer).useUntil(signal, rc))

  def createServer: Resource[IO, Server] =
    for {
      _      <- Resource.eval(certificateLoader.run)
      _      <- Resource.eval(sentryInitializer.run)
      result <- httpServer.createServer
    } yield result
}
