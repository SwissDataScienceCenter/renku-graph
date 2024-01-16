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
import io.renku.entities.searchgraphs.datasets.DatasetsGraphProvisioner
import io.renku.entities.searchgraphs.projects.ProjectsGraphProvisioner
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.Project
import io.renku.lock.Lock
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait SearchGraphsProvisioner[F[_]] {
  def provisionSearchGraphs(project: Project): F[Unit]
}

object SearchGraphsProvisioner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](lock:             Lock[F, datasets.TopmostSameAs],
                                                          connectionConfig: ProjectsConnectionConfig
  ): SearchGraphsProvisioner[F] =
    new SearchGraphsProvisionerImpl(ProjectsGraphProvisioner[F](connectionConfig),
                                    DatasetsGraphProvisioner[F](lock, connectionConfig)
    )
}

private class SearchGraphsProvisionerImpl[F[_]: MonadThrow: Logger](
    projectsGraphProvisioner: ProjectsGraphProvisioner[F],
    datasetsGraphProvisioner: DatasetsGraphProvisioner[F]
) extends SearchGraphsProvisioner[F] {

  override def provisionSearchGraphs(project: Project): F[Unit] =
    provisionProjectsGraph(project).attemptT.foldF(
      err => provisionDatasetsGraph(project).attemptT.foldF(_ => err.raiseError[F, Unit], _ => err.raiseError[F, Unit]),
      _ => provisionDatasetsGraph(project)
    )

  private def provisionProjectsGraph(project: Project) =
    projectsGraphProvisioner
      .provisionProjectsGraph(project)
      .onError(logError(graph = "Projects"))

  private def provisionDatasetsGraph(project: Project) =
    datasetsGraphProvisioner
      .provisionDatasetsGraph(project)
      .onError(logError(graph = "Datasets"))

  private def logError(graph: String): PartialFunction[Throwable, F[Unit]] =
    Logger[F].error(_)(s"Provisioning $graph graph failed")
}
