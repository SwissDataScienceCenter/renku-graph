/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.effect.{ExitCode, IO, Spawn}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.http.client.GitLabClient
import io.renku.http.server.HttpServer
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.microservices.{IOMicroservice, ServiceReadinessChecker}
import org.typelevel.log4cats.Logger

object Microservice extends IOMicroservice {

  val ServicePort:             Int Refined Positive = 9006
  private implicit val logger: Logger[IO]           = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] =
    MetricsRegistry[IO]().flatMap { implicit metricsRegistry =>
      for {
        certificateLoader     <- CertificateLoader[IO]
        sentryInitializer     <- SentryInitializer[IO]
        gitLabClient          <- GitLabClient[IO]()
        executionTimeRecorder <- ExecutionTimeRecorder[IO]()
        commitSyncCategory <-
          events.categories.commitsync.SubscriptionFactory(gitLabClient, executionTimeRecorder)
        globalCommitSyncCategory <-
          events.categories.globalcommitsync.SubscriptionFactory(gitLabClient, executionTimeRecorder)
        eventConsumersRegistry  <- consumers.EventConsumersRegistry(commitSyncCategory, globalCommitSyncCategory)
        serviceReadinessChecker <- ServiceReadinessChecker[IO](ServicePort)
        microserviceRoutes      <- MicroserviceRoutes(eventConsumersRegistry, new RoutesMetrics[IO])
        exitcode <- microserviceRoutes.routes.use { routes =>
                      new MicroserviceRunner[IO](serviceReadinessChecker,
                                                 certificateLoader,
                                                 sentryInitializer,
                                                 eventConsumersRegistry,
                                                 HttpServer[IO](serverPort = ServicePort.value, routes)
                      ).run()
                    }
      } yield exitcode
    }
}

class MicroserviceRunner[F[_]: Spawn: Logger](serviceReadinessChecker: ServiceReadinessChecker[F],
                                              certificateLoader:      CertificateLoader[F],
                                              sentryInitializer:      SentryInitializer[F],
                                              eventConsumersRegistry: EventConsumersRegistry[F],
                                              httpServer:             HttpServer[F]
) {

  def run(): F[ExitCode] = for {
    _      <- certificateLoader.run()
    _      <- sentryInitializer.run()
    _      <- Spawn[F].start(serviceReadinessChecker.waitIfNotUp >> eventConsumersRegistry.run())
    result <- httpServer.run()
  } yield result
}
