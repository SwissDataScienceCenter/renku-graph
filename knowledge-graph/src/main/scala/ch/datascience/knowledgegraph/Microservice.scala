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

package ch.datascience.knowledgegraph

import java.util.concurrent.Executors.newFixedThreadPool

import cats.effect._
import ch.datascience.config.GitLab
import ch.datascience.config.sentry.SentryInitializer
import ch.datascience.control.{RateLimit, Throttler}
import ch.datascience.http.server.HttpServer
import ch.datascience.knowledgegraph.datasets.rest._
import ch.datascience.knowledgegraph.graphql.IOQueryEndpoint
import ch.datascience.knowledgegraph.projects.rest.IOProjectEndpoint
import ch.datascience.logging.ApplicationLogger
import ch.datascience.metrics.{MetricsRegistry, RoutesMetrics}
import ch.datascience.microservices.IOMicroservice
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
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
      sentryInitializer       <- SentryInitializer[IO]
      gitLabRateLimit         <- RateLimit.fromConfig[IO, GitLab]("services.gitlab.rate-limit")
      gitLabThrottler         <- Throttler[IO, GitLab](gitLabRateLimit)
      metricsRegistry         <- MetricsRegistry()
      sparqlTimeRecorder      <- SparqlQueryTimeRecorder(metricsRegistry)
      projectEndpoint         <- IOProjectEndpoint(gitLabThrottler, sparqlTimeRecorder)
      projectDatasetsEndpoint <- IOProjectDatasetsEndpoint(sparqlTimeRecorder)
      queryEndpoint           <- IOQueryEndpoint(sparqlTimeRecorder, ApplicationLogger)
      datasetEndpoint         <- IODatasetEndpoint(sparqlTimeRecorder)
      datasetsSearchEndpoint  <- IODatasetsSearchEndpoint(sparqlTimeRecorder)
      routes <- new MicroserviceRoutes[IO](
                 queryEndpoint,
                 projectEndpoint,
                 projectDatasetsEndpoint,
                 datasetEndpoint,
                 datasetsSearchEndpoint,
                 new RoutesMetrics[IO](metricsRegistry)
               ).routes
      httpServer = new HttpServer[IO](serverPort = 9004, routes)

      exitCode <- new MicroserviceRunner(sentryInitializer, httpServer).run(args)
    } yield exitCode
}

private class MicroserviceRunner(
    sentryInitializer: SentryInitializer[IO],
    httpServer:        HttpServer[IO]
) {

  def run(args: List[String]): IO[ExitCode] =
    for {
      _        <- sentryInitializer.run
      exitCode <- httpServer.run
    } yield exitCode
}
