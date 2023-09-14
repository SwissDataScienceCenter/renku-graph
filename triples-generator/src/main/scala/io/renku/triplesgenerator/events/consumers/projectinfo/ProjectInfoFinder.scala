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

package io.renku.triplesgenerator.events.consumers.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.model.gitlab.{GitLabMember, GitLabProjectInfo}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Role
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[consumers] trait ProjectInfoFinder[F[_]] {
  def findProjectInfo(slug: projects.Slug)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private[consumers] object ProjectInfoFinder {
  def apply[F[_]: Async: NonEmptyParallel: Parallel: GitLabClient: Logger]: F[ProjectInfoFinder[F]] = for {
    projectFinder     <- ProjectFinder[F]
    membersFinder     <- ProjectMembersFinder[F]
    memberEmailFinder <- MemberEmailFinder[F]
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
      slug: projects.Slug
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]] =
    findProject(slug) >>= {
      case None          => EitherT.rightT[F, ProcessingRecoverableError](Option.empty[GitLabProjectInfo])
      case Some(project) => (addMembers(project) >>= addEmails).map(_.some)
    }

  private def addMembers(project: GitLabProjectInfo)(implicit maybeAccessToken: Option[AccessToken]) =
    findProjectMembers(project.slug).map(members => project.copy(members = members))

  private def addEmails(project: GitLabProjectInfo)(implicit maybeAccessToken: Option[AccessToken]) = {
    val proj = Project(project.id, project.slug)
    val forMembers = project.members.toList
      .parTraverse(findMemberEmail(_, proj))
      .map(deduplicateSameIdMembers)

    val forCreator =
      project.maybeCreator.toList
        .traverse(user => findMemberEmail(user.toMember(Role.Reader), proj))
        .map(_.headOption.map(_.asUser))

    forCreator
      .map(user => user.map(u => project.copy(maybeCreator = u.some)).getOrElse(project))
      .flatMap(p => forMembers.map(members => updateCreator(members).andThen(updateMembers(members))(p)))
  }

  private lazy val deduplicateSameIdMembers: List[GitLabMember] => List[GitLabMember] =
    _.foldLeft(List.empty[GitLabMember]) { (deduplicated, member) =>
      deduplicated.find(_.gitLabId == member.gitLabId) match {
        case None                         => member :: deduplicated
        case Some(p) if p.email.isDefined => deduplicated
        case Some(existing)               => member :: deduplicated.filterNot(_ == existing)
      }
    }

  private def updateCreator(members: List[GitLabMember]): GitLabProjectInfo => GitLabProjectInfo = project =>
    project.maybeCreator
      .flatMap(creator =>
        members.find(_.gitLabId == creator.gitLabId).map(member => project.copy(maybeCreator = member.asUser.some))
      )
      .getOrElse(project)

  private def updateMembers(members: List[GitLabMember]): GitLabProjectInfo => GitLabProjectInfo = project =>
    project.copy(
      members = members.filter(member => project.members.exists(_.gitLabId == member.gitLabId)).toSet
    )
}
