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

package io.renku.triplesgenerator

import cats.effect.IO
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.projects.Role
import io.renku.graph.model.{RenkuUrl, entities}
import io.renku.logging.{ExecutionTimeRecorder, TestExecutionTimeRecorder}
import io.renku.projectauth.ProjectMember
import io.renku.triplesgenerator.events.consumers.ProjectAuthSync
import io.renku.triplesstore._

trait DatasetProvision extends SearchInfoDatasets { self: ProjectsDataset with InMemoryJena =>

  override def provisionProject(
      project: entities.Project
  )(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ): IO[Unit] = {
    val execTimeRecorder: ExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    implicit val sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[IO] =
      new SparqlQueryTimeRecorder[IO](execTimeRecorder)
    val ps      = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(ProjectAuthSync[IO](_))
    val members = project.members.flatMap(p => p.maybeGitLabId.map(id => ProjectMember(id, Role.Reader)))
    super.provisionProject(project) *> ps.use(_.syncProject(project.slug, members))
  }
}
