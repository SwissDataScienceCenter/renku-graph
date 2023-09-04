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

package io.renku.knowledgegraph

import cats.effect.IO
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.projects.Role
import io.renku.graph.model.{RenkuUrl, entities, testentities}
import io.renku.projectauth.{ProjectAuthData, ProjectAuthService, ProjectMember}
import io.renku.triplesstore._

trait DatasetProvision extends SearchInfoDatasets { self: ProjectsDataset with InMemoryJena =>

  def projectAuthServiceR(implicit renkuUrl: RenkuUrl) =
    ProjectAuthService.resource[IO](projectsDSConnectionInfo.toCC())

  override def provisionProject(
      project: entities.Project
  )(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ): IO[Unit] = {
    val members  = project.members.flatMap(p => p.maybeGitLabId.map(id => ProjectMember(id, Role.Reader)))
    val authData = ProjectAuthData(project.slug, members, project.visibility)
    super.provisionProject(project) *> projectAuthServiceR.use(_.update(authData))
  }

  def provisionProjectAndMembers(
      project:    entities.Project,
      memberRole: PartialFunction[entities.Person, Role] = PartialFunction.empty
  )(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ) = {
    val members = project.members.flatMap(p =>
      p.maybeGitLabId.map(gid => ProjectMember(gid, memberRole.lift.apply(p).getOrElse(Role.Reader)))
    )
    val authData = ProjectAuthData(project.slug, members, project.visibility)
    super.provisionProject(project) *> projectAuthServiceR.use(_.update(authData))
  }

  def provisionTestProjectAndMembers(
      project:    testentities.Project,
      memberRole: PartialFunction[testentities.Person, Role] = PartialFunction.empty
  )(implicit
      entityFunctions: EntityFunctions[entities.Project],
      graphsProducer:  GraphsProducer[entities.Project],
      renkuUrl:        RenkuUrl
  ) = {
    val members = project.members.flatMap(p =>
      p.maybeGitLabId.map(gid => ProjectMember(gid, memberRole.lift.apply(p).getOrElse(Role.Reader)))
    )
    val authData = ProjectAuthData(project.slug, members, project.visibility)
    super.provisionTestProject(project) *> projectAuthServiceR.use(_.update(authData))
  }
}
