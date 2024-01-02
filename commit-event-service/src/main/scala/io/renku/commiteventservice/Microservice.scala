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

package io.renku.commiteventservice

import cats.effect.{Async, ExitCode, IO, Ref, Resource, Spawn}
import cats.syntax.all._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.consumers
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.http.client.GitLabClient
import io.renku.http.server.HttpServer
import io.renku.logging.{ApplicationLogger, ExecutionTimeRecorder}
import io.renku.metrics.{MetricsRegistry, RoutesMetrics}
import io.renku.microservices.{IOMicroservice, ResourceUse, ServiceReadinessChecker}
import org.typelevel.log4cats.Logger
import com.comcast.ip4s._
import fs2.concurrent.{Signal, SignallingRef}
import org.http4s.server.Server

object Microservice extends IOMicroservice {

  val ServicePort:             Port       = port"9006"
  private implicit val logger: Logger[IO] = ApplicationLogger

  override def run(args: List[String]): IO[ExitCode] = for {
    implicit0(mr: MetricsRegistry[IO])        <- MetricsRegistry[IO]()
    implicit0(etr: ExecutionTimeRecorder[IO]) <- ExecutionTimeRecorder[IO]()
    implicit0(gc: GitLabClient[IO])           <- GitLabClient[IO]()
    certificateLoader                         <- CertificateLoader[IO]
    sentryInitializer                         <- SentryInitializer[IO]
    commitSyncCategory                        <- events.consumers.commitsync.SubscriptionFactory[IO]
    globalCommitSyncCategory                  <- events.consumers.globalcommitsync.SubscriptionFactory[IO]
    eventConsumersRegistry  <- consumers.EventConsumersRegistry(commitSyncCategory, globalCommitSyncCategory)
    serviceReadinessChecker <- ServiceReadinessChecker[IO](ServicePort)
    microserviceRoutes      <- MicroserviceRoutes(eventConsumersRegistry, new RoutesMetrics[IO])
    termSignal              <- SignallingRef.of[IO, Boolean](false)
    exitcode <- microserviceRoutes.routes.use { routes =>
                  new MicroserviceRunner[IO](serviceReadinessChecker,
                                             certificateLoader,
                                             sentryInitializer,
                                             eventConsumersRegistry,
                                             HttpServer[IO](serverPort = ServicePort, routes)
                  ).run(termSignal)
                }
  } yield exitcode
}

class MicroserviceRunner[F[_]: Async: Logger](serviceReadinessChecker: ServiceReadinessChecker[F],
                                              certificateLoader:      CertificateLoader[F],
                                              sentryInitializer:      SentryInitializer[F],
                                              eventConsumersRegistry: EventConsumersRegistry[F],
                                              httpServer:             HttpServer[F]
) {

  def run(signal: Signal[F, Boolean]): F[ExitCode] =
    Ref.of[F, ExitCode](ExitCode.Success).flatMap(rc => ResourceUse(createServer).useUntil(signal, rc))

  def createServer: Resource[F, Server] =
    for {
      _      <- Resource.eval(certificateLoader.run)
      _      <- Resource.eval(sentryInitializer.run)
      _      <- Spawn[F].background(serviceReadinessChecker.waitIfNotUp >> eventConsumersRegistry.run)
      result <- httpServer.createServer
    } yield result
}
