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

package io.renku.entities.searchgraphs

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs.datasets.DatasetsGraphCleaner
import io.renku.entities.searchgraphs.projects.ProjectsGraphCleaner
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.lock.Lock
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait SearchGraphsCleaner[F[_]] {
  def cleanSearchGraphs(projectIdentification: ProjectIdentification): F[Unit]
}

object SearchGraphsCleaner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](lock:             Lock[F, datasets.TopmostSameAs],
                                                          connectionConfig: ProjectsConnectionConfig
  ): SearchGraphsCleaner[F] =
    new SearchGraphsCleanerImpl[F](ProjectsGraphCleaner[F](connectionConfig),
                                   DatasetsGraphCleaner[F](lock, connectionConfig)
    )
}

private class SearchGraphsCleanerImpl[F[_]: MonadThrow: Logger](
    projectsGraphCleaner: ProjectsGraphCleaner[F],
    datasetsGraphCleaner: DatasetsGraphCleaner[F]
) extends SearchGraphsCleaner[F] {

  override def cleanSearchGraphs(projectIdentification: ProjectIdentification): F[Unit] =
    cleanProjectsGraph(projectIdentification).attemptT.foldF(
      err =>
        cleanDatasetsGraph(projectIdentification).attemptT
          .foldF(_ => err.raiseError[F, Unit], _ => err.raiseError[F, Unit]),
      _ => cleanDatasetsGraph(projectIdentification)
    )

  private def cleanProjectsGraph(projectIdentification: ProjectIdentification) =
    projectsGraphCleaner
      .cleanProjectsGraph(projectIdentification)
      .onError(logError(graph = "Projects"))

  private def cleanDatasetsGraph(projectIdentification: ProjectIdentification) =
    datasetsGraphCleaner
      .cleanDatasetsGraph(projectIdentification)
      .onError(logError(graph = "Datasets"))

  private def logError(graph: String): PartialFunction[Throwable, F[Unit]] =
    Logger[F].error(_)(s"Cleaning $graph graph failed")
}
