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

package ch.datascience.commiteventservice

import cats.effect.{CancelToken, ContextShift, ExitCode, Fiber, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.metrics.MetricsRegistry
import ch.datascience.microservices.IOMicroservice
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import pureconfig.ConfigSource

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors.newFixedThreadPool
import scala.concurrent.ExecutionContext

object Microservice extends IOMicroservice {

  val ServicePort: Int Refined Positive = 9006

  protected implicit override val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] = IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] = IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] = for {
    certificateLoader     <- CertificateLoader[IO](ApplicationLogger)
    sentryInitializer     <- SentryInitializer[IO]()
    gitLabRateLimit       <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler       <- Throttler[IO, GitLab](gitLabRateLimit)
    executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger)
    metricsRegistry       <- MetricsRegistry()
    commitSyncCategory <-
      events.categories.commitsync.SubscriptionFactory(gitLabThrottler, ApplicationLogger, executionTimeRecorder)
    globalCommitSyncCategory <-
      events.categories.globalcommitsync.SubscriptionFactory(gitLabThrottler, ApplicationLogger, executionTimeRecorder)
    eventConsumersRegistry <- consumers.EventConsumersRegistry(commitSyncCategory, globalCommitSyncCategory)
    microserviceRoutes     <- MicroserviceRoutes(eventConsumersRegistry, metricsRegistry)
    exitcode <- microserviceRoutes.routes.use { routes =>
                  val httpServer = new HttpServer[IO](serverPort = ServicePort.value, routes)

                  new MicroserviceRunner(
                    certificateLoader,
                    sentryInitializer,
                    eventConsumersRegistry,
                    httpServer,
                    subProcessesCancelTokens
                  ).run()
                }
  } yield exitcode
}

class MicroserviceRunner(certificateLoader:        CertificateLoader[IO],
                         sentryInitializer:        SentryInitializer[IO],
                         eventConsumersRegistry:   EventConsumersRegistry[IO],
                         httpServer:               HttpServer[IO],
                         subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:                           ContextShift[IO]) {

  def run(): IO[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- eventConsumersRegistry.run().start map gatherCancelToken
    result <- httpServer.run()
  } yield result

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
