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
import cats.data.EitherT.rightT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.typelevel.log4cats.Logger

private trait MemberEmailFinder[F[_]] {
  def findMemberEmail(member: ProjectMember, project: Project)(implicit
      maybeAccessToken:       Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, ProjectMember]
}

private object MemberEmailFinder {
  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[MemberEmailFinder[F]] = for {
    commitAuthorFinder  <- CommitAuthorFinder[F](gitLabClient)
    projectEventsFinder <- ProjectEventsFinder[F](gitLabClient)
  } yield new MemberEmailFinderImpl(commitAuthorFinder, projectEventsFinder)
}

private class MemberEmailFinderImpl[F[_]: Async: Logger](
    commitAuthorFinder:  CommitAuthorFinder[F],
    projectEventsFinder: ProjectEventsFinder[F],
    recoveryStrategy:    RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends MemberEmailFinder[F] {

  import commitAuthorFinder._

  override def findMemberEmail(member: ProjectMember, project: Project)(implicit
      maybeAccessToken:                Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, ProjectMember] = EitherT {
    member match {
      case member: ProjectMemberWithEmail =>
        member.asRight[ProcessingRecoverableError].widen[ProjectMember].pure[F]
      case member: ProjectMemberNoEmail =>
        findInCommitsAndEvents(member, project).value
          .recoverWith(recoveryStrategy.maybeRecoverableError[F, ProjectMember])
    }
  }

  private def findInCommitsAndEvents(member:  ProjectMemberNoEmail,
                                     project: Project,
                                     paging:  PagingInfo = PagingInfo(maybeNextPage = Some(1), maybeTotalPages = None)
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, ProjectMember] =
    paging.findNextPage match {
      case None => rightT[F, ProcessingRecoverableError](member)
      case Some(nextPage) =>
        for {
          eventsAndNextPage <- projectEventsFinder.find(project, nextPage).map(filterEventsFor(member))
          maybeEmail        <- matchEmailFromCommits(eventsAndNextPage._1, project)
          updatedMember     <- addEmailOrCheckNextPage(member, maybeEmail, project, eventsAndNextPage._2)
        } yield updatedMember
    }

  private def filterEventsFor(
      member: ProjectMember
  ): ((List[PushEvent], PagingInfo)) => (List[PushEvent], PagingInfo) = { case (events, paging) =>
    events.filter(ev => ev.authorId == member.gitLabId) -> paging
  }

  private def addEmailOrCheckNextPage(member:     ProjectMemberNoEmail,
                                      maybeEmail: Option[persons.Email],
                                      project:    Project,
                                      paging:     PagingInfo
  )(implicit maybeAccessToken:                    Option[AccessToken]) = maybeEmail match {
    case None        => findInCommitsAndEvents(member, project, paging)
    case Some(email) => rightT[F, ProcessingRecoverableError](member add email)
  }

  private def matchEmailFromCommits(events:  List[PushEvent],
                                    project: Project,
                                    maybeEmail: EitherT[F, ProcessingRecoverableError, Option[persons.Email]] =
                                      rightT[F, ProcessingRecoverableError](Option.empty[persons.Email])
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, Option[persons.Email]] =
    maybeEmail >>= {
      case someEmail @ Some(_) => rightT[F, ProcessingRecoverableError](someEmail)
      case none =>
        events match {
          case Nil                    => rightT[F, ProcessingRecoverableError](none)
          case event :: eventsToCheck => matchEmailOnSingleCommit(event, project, eventsToCheck)
        }
    }

  private def matchEmailOnSingleCommit(event: PushEvent, project: Project, eventsToCheck: List[PushEvent])(implicit
      maybeAccessToken:                       Option[AccessToken]
  ) = findCommitAuthor(project.path, event.commitId) >>= {
    case Some((event.authorName, email)) => rightT[F, ProcessingRecoverableError](email.some)
    case _ => matchEmailFromCommits(eventsToCheck, project, rightT[F, ProcessingRecoverableError](none))
  }
}

private final case class Project(id: projects.Id, path: projects.Path)
