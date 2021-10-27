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

package io.renku.webhookservice

import cats.effect._
import io.renku.config.GitLab
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.control.{RateLimit, Throttler}
import io.renku.http.server.HttpServer
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.IOMicroservice

object Microservice extends IOMicroservice {

  private implicit val logger: ApplicationLogger.type = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    certificateLoader     <- CertificateLoader[IO]
    sentryInitializer     <- SentryInitializer[IO]
    gitLabRateLimit       <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler       <- Throttler[IO, GitLab](gitLabRateLimit)
    executionTimeRecorder <- ExecutionTimeRecorder[IO]()
    metricsRegistry       <- MetricsRegistry[IO]()
    microserviceRoutes <-
      MicroserviceRoutes(metricsRegistry, gitLabThrottler, executionTimeRecorder)
    exitcode <- microserviceRoutes.routes.use { routes =>
                  val httpServer = HttpServer[IO](serverPort = 9001, routes)
                  new MicroserviceRunner(
                    certificateLoader,
                    sentryInitializer,
                    httpServer
                  ).run()
                }
  } yield exitcode
}

class MicroserviceRunner(certificateLoader: CertificateLoader[IO],
                         sentryInitializer: SentryInitializer[IO],
                         httpServer:        HttpServer[IO]
) {

  def run(): IO[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    result <- httpServer.run()
  } yield result
}
