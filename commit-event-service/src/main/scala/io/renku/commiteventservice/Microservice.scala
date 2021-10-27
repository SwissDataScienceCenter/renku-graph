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

package io.renku.commiteventservice

import cats.effect.{ExitCode, FiberIO, IO}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.config.GitLab
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.control.{RateLimit, Throttler}
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.http.server.HttpServer
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.microservices.IOMicroservice
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentHashMap

object Microservice extends IOMicroservice {

  val ServicePort:             Int Refined Positive = 9006
  private implicit val logger: Logger[IO]           = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    certificateLoader     <- CertificateLoader[IO]
    sentryInitializer     <- SentryInitializer[IO]
    gitLabRateLimit       <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler       <- Throttler[IO, GitLab](gitLabRateLimit)
    executionTimeRecorder <- ExecutionTimeRecorder[IO]()
    commitSyncCategory <-
      events.categories.commitsync.SubscriptionFactory(gitLabThrottler, executionTimeRecorder)
    globalCommitSyncCategory <-
      events.categories.globalcommitsync.SubscriptionFactory(gitLabThrottler, executionTimeRecorder)
    eventConsumersRegistry <- consumers.EventConsumersRegistry(commitSyncCategory, globalCommitSyncCategory)
    metricsRegistry        <- MetricsRegistry[IO]()
    microserviceRoutes     <- MicroserviceRoutes(eventConsumersRegistry, new RoutesMetrics[IO](metricsRegistry))
    exitcode <- microserviceRoutes.routes.use { routes =>
                  new MicroserviceRunner(
                    certificateLoader,
                    sentryInitializer,
                    eventConsumersRegistry,
                    HttpServer[IO](serverPort = ServicePort.value, routes),
                    subProcessesCancelTokens
                  ).run()
                }
  } yield exitcode
}

class MicroserviceRunner(certificateLoader:        CertificateLoader[IO],
                         sentryInitializer:        SentryInitializer[IO],
                         eventConsumersRegistry:   EventConsumersRegistry[IO],
                         httpServer:               HttpServer[IO],
                         subProcessesCancelTokens: ConcurrentHashMap[IO[Unit], Unit]
) {

  def run(): IO[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- eventConsumersRegistry.run().start map gatherCancelToken
    result <- httpServer.run()
  } yield result

  private def gatherCancelToken(fiber: FiberIO[Unit]): FiberIO[Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
