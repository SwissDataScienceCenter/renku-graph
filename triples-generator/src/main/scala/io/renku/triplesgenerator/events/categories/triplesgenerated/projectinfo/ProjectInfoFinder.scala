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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[triplesgenerated] trait ProjectInfoFinder[F[_]] {
  def findProjectInfo(path: projects.Path)(implicit
      maybeAccessToken:     Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private[triplesgenerated] object ProjectInfoFinder {
  def apply[F[_]: Async: NonEmptyParallel: Parallel: Logger](
      gitLabThrottler: Throttler[F, GitLab]
  ): F[ProjectInfoFinder[F]] =
    for {
      projectFinder     <- ProjectFinder[F](gitLabThrottler)
      membersFinder     <- ProjectMembersFinder[F](gitLabThrottler)
      memberEmailFinder <- MemberEmailFinder[F](gitLabThrottler)
    } yield new ProjectInfoFinderImpl(projectFinder, membersFinder, memberEmailFinder)
}

private[triplesgenerated] class ProjectInfoFinderImpl[F[_]: MonadThrow: Parallel: Logger](
    projectFinder:     ProjectFinder[F],
    membersFinder:     ProjectMembersFinder[F],
    memberEmailFinder: MemberEmailFinder[F]
) extends ProjectInfoFinder[F] {

  import membersFinder._
  import projectFinder._
  import memberEmailFinder._

  override def findProjectInfo(
      path:                    projects.Path
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]] =
    findProject(path) >>= {
      case None          => EitherT.rightT[F, ProcessingRecoverableError](Option.empty[GitLabProjectInfo])
      case Some(project) => (addMembers(project) >>= addEmails).map(_.some)
    }

  private def addMembers(project: GitLabProjectInfo)(implicit maybeAccessToken: Option[AccessToken]) =
    findProjectMembers(project.path).map(members => project.copy(members = members))

  private def addEmails(project: GitLabProjectInfo)(implicit maybeAccessToken: Option[AccessToken]) = EitherT {
    (project.members ++ project.maybeCreator).toList
      .map(findMemberEmail(_, Project(project.id, project.path)).value)
      .parSequence
      .map(_.sequence)
  }.map(members => (updateCreator(members) andThen updateMembers(members))(project))

  private def updateCreator(members: List[ProjectMember]): GitLabProjectInfo => GitLabProjectInfo = project =>
    project.maybeCreator
      .flatMap(creator =>
        members.find(_.gitLabId == creator.gitLabId).map(member => project.copy(maybeCreator = member.some))
      )
      .getOrElse(project)

  private def updateMembers(members: List[ProjectMember]): GitLabProjectInfo => GitLabProjectInfo = project =>
    project.copy(
      members = members.filter(member => project.members.exists(_.gitLabId == member.gitLabId)).toSet
    )
}
