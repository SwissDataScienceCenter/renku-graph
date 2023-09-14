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

package io.renku.triplesgenerator.events.consumers.minprojectinfo

import cats.data.{EitherT, ValidatedNel}
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.entities.Project
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects.ResourceId
import io.renku.graph.model.{RenkuUrl, entities, persons}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.errors.{ProcessingNonRecoverableError, ProcessingRecoverableError}
import io.renku.triplesgenerator.events.consumers.projectinfo.ProjectInfoFinder
import org.typelevel.log4cats.Logger

private trait EntityBuilder[F[_]] {
  def buildEntity(event: MinProjectInfoEvent)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Project]
}

private class EntityBuilderImpl[F[_]: MonadThrow](projectInfoFinder: ProjectInfoFinder[F])(implicit renkuUrl: RenkuUrl)
    extends EntityBuilder[F] {

  import projectInfoFinder._

  override def buildEntity(event: MinProjectInfoEvent)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Project] =
    findGLProject(event) >>= toProject

  private def findGLProject(event: MinProjectInfoEvent)(implicit mat: Option[AccessToken]) =
    findProjectInfo(event.project.slug)
      .semiflatMap {
        case Some(projectInfo) => projectInfo.pure[F]
        case None =>
          ProcessingNonRecoverableError
            .MalformedRepository(show"${event.project} not found in GitLab")
            .raiseError[F, GitLabProjectInfo]
      }

  private def toProject(info: GitLabProjectInfo) =
    EitherT
      .fromEither[F](convert(info).toEither)
      .leftSemiflatMap(err =>
        ProcessingNonRecoverableError
          .MalformedRepository(err.intercalate("; "))
          .raiseError[F, ProcessingRecoverableError]
      )

  private lazy val convert: GitLabProjectInfo => ValidatedNel[String, Project] = {
    case GitLabProjectInfo(_,
                           name,
                           slug,
                           dateCreated,
                           dateModified,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           Some(parentSlug),
                           avatarUrl
        ) =>
      entities.NonRenkuProject.WithParent.from(
        ResourceId(slug),
        slug,
        name,
        maybeDescription,
        dateCreated,
        dateModified,
        maybeCreator.map(toMember).map(_.person),
        visibility,
        keywords,
        members.map(toMember),
        ResourceId(parentSlug),
        avatarUrl.map(Image.projectImage(ResourceId(slug), _)).toList
      )
    case GitLabProjectInfo(_,
                           name,
                           slug,
                           dateCreated,
                           dateModified,
                           maybeDescription,
                           maybeCreator,
                           keywords,
                           members,
                           visibility,
                           None,
                           avatarUrl
        ) =>
      entities.NonRenkuProject.WithoutParent.from(
        ResourceId(slug),
        slug,
        name,
        maybeDescription,
        dateCreated,
        dateModified,
        maybeCreator.map(toMember).map(_.person),
        visibility,
        keywords,
        members.map(toMember),
        avatarUrl.map(Image.projectImage(ResourceId(slug), _)).toList
      )
  }

  private def toMember(projectMember: ProjectMember): Project.Member = projectMember match {
    case ProjectMemberNoEmail(name, _, gitLabId, _) =>
      Project.Member(
        entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                     gitLabId,
                                     name,
                                     maybeEmail = None,
                                     maybeOrcidId = None,
                                     maybeAffiliation = None
        ),
        projectMember.role
      )
    case ProjectMemberWithEmail(name, _, gitLabId, email, _) =>
      Project.Member(
        entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                     gitLabId,
                                     name,
                                     email.some,
                                     maybeOrcidId = None,
                                     maybeAffiliation = None
        ),
        projectMember.role
      )
  }
}

private object EntityBuilder {
  def apply[F[_]: Async: NonEmptyParallel: Parallel: GitLabClient: Logger]: F[EntityBuilder[F]] = for {
    implicit0(renkuUrl: RenkuUrl) <- RenkuUrlLoader[F]()
    projectInfoFinder             <- ProjectInfoFinder[F]
  } yield new EntityBuilderImpl[F](projectInfoFinder)
}
