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

package ch.datascience.dbeventlog

import java.util.concurrent.ConcurrentHashMap

import cats.effect._
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.db.DbTransactorResource
import ch.datascience.dbeventlog.creation.IOEventCreationEndpoint
import ch.datascience.dbeventlog.init.IODbInitializer
import ch.datascience.dbeventlog.latestevents.IOLatestEventsEndpoint
import ch.datascience.dbeventlog.metrics.{EventLogMetrics, IOEventLogMetrics}
import ch.datascience.http.server.HttpServer
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice

import scala.language.higherKinds

object Microservice extends IOMicroservice {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new EventLogDbConfigProvider[IO] map DbTransactorResource[IO, EventLogDB]
      exitCode           <- runMicroservice(transactorResource, args)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, EventLogDB], args: List[String]) =
    transactorResource.use { transactor =>
      for {
        sentryInitializer     <- SentryInitializer[IO]
        metricsRegistry       <- MetricsRegistry()
        eventLogMetrics       <- IOEventLogMetrics(transactor, ApplicationLogger, metricsRegistry)
        latestEventsEndpoint  <- IOLatestEventsEndpoint(transactor, ApplicationLogger)
        eventCreationEndpoint <- IOEventCreationEndpoint(transactor, ApplicationLogger)
        routes <- new MicroserviceRoutes[IO](
                   latestEventsEndpoint,
                   eventCreationEndpoint,
                   new RoutesMetrics[IO](metricsRegistry)
                 ).routes
        httpServer = new HttpServer[IO](serverPort = 9005, routes)

        exitCode <- new MicroserviceRunner(
                     sentryInitializer,
                     new IODbInitializer(transactor, ApplicationLogger),
                     eventLogMetrics,
                     httpServer,
                     subProcessesCancelTokens
                   ) run args
      } yield exitCode
    }
}

private class MicroserviceRunner(
    sentryInitializer:        SentryInitializer[IO],
    dbInitializer:            IODbInitializer,
    metrics:                  EventLogMetrics,
    httpServer:               HttpServer[IO],
    subProcessesCancelTokens: ConcurrentHashMap[CancelToken[IO], Unit]
)(implicit contextShift:      ContextShift[IO]) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _      <- sentryInitializer.run
      _      <- dbInitializer.run
      _      <- metrics.run.start.map(gatherCancelToken)
      result <- httpServer.run
    } yield result

  private def gatherCancelToken(fiber: Fiber[IO, Unit]): Fiber[IO, Unit] = {
    subProcessesCancelTokens.put(fiber.cancel, ())
    fiber
  }
}
