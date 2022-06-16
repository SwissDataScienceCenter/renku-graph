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

package io.renku.triplesgenerator.events.categories.tsprovisioning.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[tsprovisioning] trait ProjectInfoFinder[F[_]] {
  def findProjectInfo(path: projects.Path)(implicit
      maybeAccessToken:     Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private[tsprovisioning] object ProjectInfoFinder {
  def apply[F[_]: Async: NonEmptyParallel: Parallel: Logger](
      gitLabClient: GitLabClient[F]
  ): F[ProjectInfoFinder[F]] = for {
    projectFinder     <- ProjectFinder[F](gitLabClient)
    membersFinder     <- ProjectMembersFinder[F](gitLabClient)
    memberEmailFinder <- MemberEmailFinder[F](gitLabClient)
  } yield new ProjectInfoFinderImpl(projectFinder, membersFinder, memberEmailFinder)
}

private class ProjectInfoFinderImpl[F[_]: MonadThrow: Parallel: Logger](
    projectFinder:     ProjectFinder[F],
    membersFinder:     ProjectMembersFinder[F],
    memberEmailFinder: MemberEmailFinder[F]
) extends ProjectInfoFinder[F] {

  import memberEmailFinder._
  import membersFinder._
  import projectFinder._

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
  }.map(deduplicateSameIdMembers)
    .map(members => (updateCreator(members) andThen updateMembers(members))(project))

  private lazy val deduplicateSameIdMembers: List[ProjectMember] => List[ProjectMember] =
    _.foldLeft(List.empty[ProjectMember]) { (deduplicated, member) =>
      deduplicated.find(_.gitLabId == member.gitLabId) match {
        case None                                 => member :: deduplicated
        case Some(_: ProjectMemberWithEmail)      => deduplicated
        case Some(existing: ProjectMemberNoEmail) => member :: deduplicated.filterNot(_ == existing)
      }
    }

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
