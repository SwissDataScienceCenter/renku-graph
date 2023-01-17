/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.entities.searchgraphs.DatasetsGraphCleaner
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.{GraphClass, projects}
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private[cleanup] trait TSCleaner[F[_]] {
  def removeTriples(projectPath: projects.Path): F[Unit]
}

private[cleanup] object TSCleaner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
      maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
      idleTimeout:    Duration = 16 minutes,
      requestTimeout: Duration = 15 minutes
  ): F[TSCleaner[F]] = (DatasetsGraphCleaner[F], ProjectsConnectionConfig[F]())
    .mapN((datasetsGraphCleaner, connectionConfig) =>
      new TSCleanerImpl[F](ProjectIdFinder[F](connectionConfig),
                           datasetsGraphCleaner,
                           connectionConfig,
                           retryInterval,
                           maxRetries,
                           idleTimeout,
                           requestTimeout
      )
    )
}

private class TSCleanerImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    projectIdFinder:      ProjectIdFinder[F],
    datasetsGraphCleaner: DatasetsGraphCleaner[F],
    connectionConfig:     ProjectsConnectionConfig,
    retryInterval:        FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:           Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:          Duration = 16 minutes,
    requestTimeout:       Duration = 15 minutes
) extends TSClientImpl(connectionConfig,
                       retryInterval = retryInterval,
                       maxRetries = maxRetries,
                       idleTimeoutOverride = idleTimeout.some,
                       requestTimeoutOverride = requestTimeout.some
    )
    with TSCleaner[F] {

  import ProjectHierarchyFixer._
  import SameAsHierarchyFixer._
  import datasetsGraphCleaner._
  import eu.timepit.refined.auto._
  import io.renku.triplesstore.SparqlQuery
  import projectIdFinder._
  private implicit val tsConnection: ProjectsConnectionConfig = connectionConfig

  override def removeTriples(projectPath: projects.Path): F[Unit] =
    findProjectId(projectPath) >>= {
      case Some(project) =>
        relinkSameAsHierarchy(project.path) >>
          relinkProjectHierarchy(project.path) >>
          removeProjectGraph(project) >>
          cleanDatasetsGraph(project)
      case None => ().pure[F]
    }

  private def removeProjectGraph(project: ProjectIdentification): F[Unit] =
    updateWithNoResult {
      SparqlQuery.of(
        name = "project graph removal",
        s"""DROP GRAPH ${GraphClass.Project.id(project.resourceId).asSparql.sparql}"""
      )
    }
}
