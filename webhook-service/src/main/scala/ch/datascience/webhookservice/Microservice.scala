/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice

import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.config.GitLab
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.{ApplicationLogger, ExecutionTimeRecorder}
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.eventprocessing.{IOHookEventEndpoint, IOProcessingStatusEndpoint}
import ch.datascience.webhookservice.hookcreation.IOHookCreationEndpoint
import ch.datascience.webhookservice.hookvalidation.IOHookValidationEndpoint
import ch.datascience.webhookservice.missedevents.{EventsSynchronizationScheduler, IOEventsSynchronizationScheduler}
import ch.datascience.webhookservice.project.ProjectHookUrl
import pureconfig.ConfigSource

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object Microservice extends IOMicroservice {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(ConfigSource.default.at("threads-number").loadOrThrow[Int])

  protected implicit override def contextShift: ContextShift[IO] =
    IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] =
    IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] = {
    for {
      sentryInitializer     <- SentryInitializer[IO].asResource
      projectHookUrl        <- ProjectHookUrl.fromConfig[IO]().asResource
      gitLabRateLimit       <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit").asResource
      gitLabThrottler       <- Throttler[IO, GitLab](gitLabRateLimit).asResource
      hookTokenCrypto       <- HookTokenCrypto[IO]().asResource
      executionTimeRecorder <- ExecutionTimeRecorder[IO](ApplicationLogger).asResource
      metricsRegistry       <- MetricsRegistry().asResource
      hookEventEndpoint <- IOHookEventEndpoint(gitLabThrottler,
                                               hookTokenCrypto,
                                               executionTimeRecorder,
                                               ApplicationLogger).asResource
      hookCreatorEndpoint <- IOHookCreationEndpoint(projectHookUrl,
                                                    gitLabThrottler,
                                                    hookTokenCrypto,
                                                    executionTimeRecorder,
                                                    ApplicationLogger).asResource
      processingStatusEndpoint <- IOProcessingStatusEndpoint(projectHookUrl,
                                                             gitLabThrottler,
                                                             executionTimeRecorder,
                                                             ApplicationLogger).asResource
      hookValidationEndpoint <- IOHookValidationEndpoint(projectHookUrl, gitLabThrottler).asResource
      routes <- new MicroserviceRoutes[IO](
                 hookEventEndpoint,
                 hookCreatorEndpoint,
                 hookValidationEndpoint,
                 processingStatusEndpoint,
                 new RoutesMetrics[IO](metricsRegistry)
               ).routes
      httpServer = new HttpServer[IO](serverPort = 9001, routes)

      eventsSynchronizationScheduler <- IOEventsSynchronizationScheduler(gitLabThrottler,
                                                                         executionTimeRecorder,
                                                                         ApplicationLogger).asResource
      exitCode = new MicroserviceRunner(
        sentryInitializer,
        eventsSynchronizationScheduler,
        httpServer
      ) run args
    } yield exitCode
  }.use(identity)
}

class MicroserviceRunner(sentryInitializer:              SentryInitializer[IO],
                         eventsSynchronizationScheduler: EventsSynchronizationScheduler[IO],
                         httpServer:                     HttpServer[IO])(implicit contextShift: ContextShift[IO]) {
  import cats.implicits._

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- sentryInitializer.run
      _ <- List(httpServer.run.start, eventsSynchronizationScheduler.run).sequence
    } yield ExitCode.Success
}
