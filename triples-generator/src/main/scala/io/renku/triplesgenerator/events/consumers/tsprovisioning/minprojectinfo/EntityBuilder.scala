/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package minprojectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.entities.Project
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects.ResourceId
import io.renku.graph.model.{RenkuUrl, entities, persons}
import io.renku.http.client.{AccessToken, GitLabClient}
import org.typelevel.log4cats.Logger
import projectinfo.ProjectInfoFinder

private trait EntityBuilder[F[_]] {
  def buildEntity(event: MinProjectInfoEvent)(implicit
      maybeAccessToken:  Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Project]
}

private class EntityBuilderImpl[F[_]: MonadThrow](projectInfoFinder: ProjectInfoFinder[F], renkuUrl: RenkuUrl)
    extends EntityBuilder[F] {

  import projectInfoFinder._
  private implicit val renkuUrlImplicit: RenkuUrl = renkuUrl

  override def buildEntity(event: MinProjectInfoEvent)(implicit
      maybeAccessToken:           Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Project] = findValidProjectInfo(event).map(toProject)

  private def findValidProjectInfo(event: MinProjectInfoEvent)(implicit
      maybeAccessToken:                   Option[AccessToken]
  ) = findProjectInfo(event.project.path) semiflatMap {
    case Some(projectInfo) => projectInfo.pure[F]
    case None =>
      ProcessingNonRecoverableError
        .MalformedRepository(show"${event.project} not found in GitLab")
        .raiseError[F, GitLabProjectInfo]
  }

  private lazy val toProject: GitLabProjectInfo => Project = {
    case GitLabProjectInfo(_,
                           name,
                           path,
                           dateCreated,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           Some(parentPath)
        ) =>
      entities.NonRenkuProject.WithParent(ResourceId(path),
                                          path,
                                          name,
                                          maybeDescription,
                                          dateCreated,
                                          maybeCreator.map(toPerson),
                                          visibility,
                                          keywords,
                                          members.map(toPerson),
                                          ResourceId(parentPath)
      )
    case GitLabProjectInfo(_,
                           name,
                           path,
                           dateCreated,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           None
        ) =>
      entities.NonRenkuProject.WithoutParent(ResourceId(path),
                                             path,
                                             name,
                                             maybeDescription,
                                             dateCreated,
                                             maybeCreator.map(toPerson),
                                             visibility,
                                             keywords,
                                             members.map(toPerson)
      )
  }

  private def toPerson(projectMember: ProjectMember): entities.Person = projectMember match {
    case ProjectMemberNoEmail(name, _, gitLabId) =>
      entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                   gitLabId,
                                   name,
                                   maybeEmail = None,
                                   maybeOrcidId = None,
                                   maybeAffiliation = None
      )
    case ProjectMemberWithEmail(name, _, gitLabId, email) =>
      entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                   gitLabId,
                                   name,
                                   email.some,
                                   maybeOrcidId = None,
                                   maybeAffiliation = None
      )
  }
}

private object EntityBuilder {
  def apply[F[_]: Async: NonEmptyParallel: Parallel: GitLabClient: Logger]: F[EntityBuilder[F]] = for {
    renkuUrl          <- RenkuUrlLoader[F]()
    projectInfoFinder <- ProjectInfoFinder[F]
  } yield new EntityBuilderImpl[F](projectInfoFinder, renkuUrl)
}
