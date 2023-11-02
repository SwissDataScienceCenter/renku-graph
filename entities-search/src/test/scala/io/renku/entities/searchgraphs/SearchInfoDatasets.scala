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

package io.renku.entities.searchgraphs

import cats.effect.IO
import cats.syntax.all._
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.projects.Role
import io.renku.graph.model.{RenkuUrl, datasets, entities, testentities}
import io.renku.lock.Lock
import io.renku.logging.{ExecutionTimeRecorder, TestExecutionTimeRecorder}
import io.renku.projectauth.{ProjectAuthData, ProjectAuthService, ProjectMember}
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

trait SearchInfoDatasets {
  self: ProjectsDataset with InMemoryJena =>

  implicit def ioLogger: Logger[IO]

  def provisionTestProjects(
      projects: testentities.Project*
  )(implicit
      renkuUrl:        RenkuUrl,
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project]
  ): IO[Unit] = projects.traverse_(provisionTestProject)

  def provisionTestProject(
      p: testentities.Project
  )(implicit
      renkuUrl:        RenkuUrl,
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project]
  ): IO[Unit] = provisionProject(p.to[entities.Project])

  def provisionProjects(projects: entities.Project*)(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ): IO[Unit] = projects.traverse_[IO, Unit](provisionProject)

  def provisionProject(
      project: entities.Project
  )(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ): IO[Unit] =
    uploadIO(projectsDataset, graphsProducer(project): _*) >> insertSearchInfo(project) >> insertProjectAuth(project)

  def insertProjectAuth(project: entities.Project)(implicit renkuUrl: RenkuUrl): IO[Unit] = {
    val execTimeRecorder: ExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    implicit val sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[IO] =
      new SparqlQueryTimeRecorder[IO](execTimeRecorder)
    val ps      = ProjectSparqlClient[IO](projectsDSConnectionInfo).map(ProjectAuthService[IO](_, renkuUrl))
    val members = project.members.flatMap(p => p.person.maybeGitLabId.map(id => ProjectMember(id, Role.Reader)))
    ps.use(_.update(ProjectAuthData(project.slug, members, project.visibility)))
  }

  def insertSearchInfo(project: entities.Project): IO[Unit] =
    createSearchGraphsProvisioner.flatMap(_.provisionSearchGraphs(project))

  def createSearchGraphsProvisioner: IO[SearchGraphsProvisioner[IO]] = {
    val execTimeRecorder: ExecutionTimeRecorder[IO] = TestExecutionTimeRecorder[IO]()
    implicit val sparqlQueryTimeRecorder: SparqlQueryTimeRecorder[IO] =
      new SparqlQueryTimeRecorder[IO](execTimeRecorder)

    IO(SearchGraphsProvisioner[IO](Lock.none[IO, datasets.TopmostSameAs], projectsDSConnectionInfo))
  }
}
