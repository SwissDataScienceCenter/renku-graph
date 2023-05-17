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

package io.renku.entities.searchgraphs.projects

import cats.effect.Async
import io.renku.entities.searchgraphs.projects.commands.ProjectInfoDeleteQuery
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private[searchgraphs] trait ProjectsGraphCleaner[F[_]] {
  def cleanProjectsGraph(project: ProjectIdentification): F[Unit]
}

private[searchgraphs] object ProjectsGraphCleaner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): ProjectsGraphCleaner[F] = new ProjectsGraphCleanerImpl[F](TSClient[F](connectionConfig))
}

private class ProjectsGraphCleanerImpl[F[_]](tsClient: TSClient[F]) extends ProjectsGraphCleaner[F] {

  override def cleanProjectsGraph(project: ProjectIdentification): F[Unit] =
    tsClient.updateWithNoResult(ProjectInfoDeleteQuery(project.resourceId))
}
