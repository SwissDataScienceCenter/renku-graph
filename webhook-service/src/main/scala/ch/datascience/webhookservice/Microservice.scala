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

package ch.datascience.webhookservice

import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.db.DbTransactorResource
import ch.datascience.dbeventlog.init.IOEventLogDbInitializer
import ch.datascience.dbeventlog.{EventLogDB, EventLogDbConfigProvider}
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.http.server.HttpServer
import ch.datascience.microservices.IOMicroservice
import ch.datascience.webhookservice.config.GitLab
import ch.datascience.webhookservice.eventprocessing.{IOHookEventEndpoint, IOProcessingStatusEndpoint}
import ch.datascience.webhookservice.hookcreation.IOHookCreationEndpoint
import ch.datascience.webhookservice.hookvalidation.IOHookValidationEndpoint
import ch.datascience.webhookservice.missedevents.{EventsSynchronizationScheduler, EventsSynchronizationThrottler, IOEventsSynchronizationScheduler}
import pureconfig.loadConfigOrThrow

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object Microservice extends IOMicroservice {

  private implicit val executionContext: ExecutionContext =
    ExecutionContext fromExecutorService newFixedThreadPool(loadConfigOrThrow[Int]("threads-number"))

  protected implicit override def contextShift: ContextShift[IO] =
    IO.contextShift(executionContext)

  protected implicit override def timer: Timer[IO] =
    IO.timer(executionContext)

  override def run(args: List[String]): IO[ExitCode] =
    for {
      transactorResource <- new EventLogDbConfigProvider[IO] map DbTransactorResource[IO, EventLogDB]
      exitCode           <- runMicroservice(transactorResource, args)
    } yield exitCode

  private def runMicroservice(transactorResource: DbTransactorResource[IO, EventLogDB], args: List[String]) =
    transactorResource.use { transactor =>
      for {
        sentryInitializer              <- SentryInitializer[IO]
        gitLabUrl                      <- GitLabUrl[IO]()
        gitLabRateLimit                <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
        gitLabThrottler                <- Throttler[IO, GitLab](gitLabRateLimit)
        eventsSynchronizationThrottler <- EventsSynchronizationThrottler[IO](gitLabRateLimit = gitLabRateLimit)

        httpServer = new HttpServer[IO](
          serverPort = 9001,
          serviceRoutes = new MicroserviceRoutes[IO](
            new IOHookEventEndpoint(transactor, gitLabUrl, gitLabThrottler),
            new IOHookCreationEndpoint(transactor, gitLabUrl, gitLabThrottler),
            new IOHookValidationEndpoint(gitLabUrl, gitLabThrottler),
            new IOProcessingStatusEndpoint(transactor, gitLabUrl, gitLabThrottler)
          ).routes
        )

        exitCode <- new MicroserviceRunner(
                     sentryInitializer,
                     new IOEventLogDbInitializer(transactor),
                     new IOEventsSynchronizationScheduler(transactor,
                                                          gitLabUrl,
                                                          gitLabThrottler,
                                                          eventsSynchronizationThrottler),
                     httpServer
                   ) run args
      } yield exitCode
    }
}

class MicroserviceRunner(sentryInitializer:              SentryInitializer[IO],
                         eventLogDbInitializer:          IOEventLogDbInitializer,
                         eventsSynchronizationScheduler: EventsSynchronizationScheduler[IO],
                         httpServer:                     HttpServer[IO])(implicit contextShift: ContextShift[IO]) {
  import cats.implicits._

  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- sentryInitializer.run
      _ <- eventLogDbInitializer.run
      _ <- List(httpServer.run.start, eventsSynchronizationScheduler.run).sequence
    } yield ExitCode.Success
}
