/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{GitLabApiUrl, projects, users}
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.http4s.Method.GET
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private trait MemberEmailFinder[F[_]] {
  def findMemberEmail(member: ProjectMember, project: Project)(implicit
      maybeAccessToken:       Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, ProjectMember]
}

private object MemberEmailFinder {
  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[MemberEmailFinder[F]] = for {
    commitAuthorFinder <- CommitAuthorFinder[F](gitLabThrottler)
    gitLabUrl          <- GitLabUrlLoader[F]()
  } yield new MemberEmailFinderImpl(commitAuthorFinder, gitLabUrl.apiV4, gitLabThrottler)
}

private class MemberEmailFinderImpl[F[_]: Async: Logger](
    commitAuthorFinder:     CommitAuthorFinder[F],
    gitLabApiUrl:           GitLabApiUrl,
    gitLabThrottler:        Throttler[F, GitLab],
    recoveryStrategy:       RecoverableErrorsRecovery = RecoverableErrorsRecovery,
    retryInterval:          FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:             Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride: Option[Duration] = None
) extends RestClient(gitLabThrottler,
                     retryInterval = retryInterval,
                     maxRetries = maxRetries,
                     requestTimeoutOverride = requestTimeoutOverride
    )
    with MemberEmailFinder[F] {

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

  private def findInCommitsAndEvents(member:        ProjectMemberNoEmail,
                                     project:       Project,
                                     maybeNextPage: Option[Int] = Some(1)
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, ProjectMember] =
    maybeNextPage match {
      case None => EitherT.rightT[F, ProcessingRecoverableError](member)
      case Some(nextPage) =>
        for {
          eventsAndNextPage <- fetchUserEvents(member, nextPage).map(filterEventsFor(member, project))
          maybeEmail        <- matchEmailFromCommits(eventsAndNextPage, project)
          updatedMember     <- addEmailOrCheckNextPage(member, maybeEmail, project, eventsAndNextPage._2)
        } yield updatedMember
    }

  private def addEmailOrCheckNextPage(member:        ProjectMemberNoEmail,
                                      maybeEmail:    Option[users.Email],
                                      project:       Project,
                                      maybeNextPage: Option[Int]
  )(implicit maybeAccessToken:                       Option[AccessToken]) = maybeEmail match {
    case None        => findInCommitsAndEvents(member, project, maybeNextPage)
    case Some(email) => EitherT.rightT[F, ProcessingRecoverableError](member add email)
  }

  private def fetchUserEvents(member: ProjectMember, nextPage: Int)(implicit maybeAccessToken: Option[AccessToken]) =
    EitherT {
      {
        for {
          uri <- validateUri(s"$gitLabApiUrl/users/${member.gitLabId}/events/?action=pushed&page=$nextPage")
          eventsAndNextPage <- send(secureRequest(GET, uri))(mapResponse)
        } yield eventsAndNextPage
      }.map(_.asRight[ProcessingRecoverableError]).recoverWith(recoveryStrategy.maybeRecoverableError)
    }

  private def filterEventsFor(member:  ProjectMember,
                              project: Project
  ): ((List[PushEvent], Option[Int])) => (List[PushEvent], Option[Int]) = { case (events, maybeNextPage) =>
    events.filter(ev => ev.projectId == project.id && ev.authorId == member.gitLabId) -> maybeNextPage
  }

  private def matchEmailFromCommits(eventsAndNextPage: (List[PushEvent], Option[Int]),
                                    project:           Project,
                                    maybeEmail: EitherT[F, ProcessingRecoverableError, Option[users.Email]] =
                                      EitherT.rightT[F, ProcessingRecoverableError](Option.empty[users.Email])
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, Option[users.Email]] =
    maybeEmail >>= {
      case someEmail @ Some(_) => EitherT.rightT[F, ProcessingRecoverableError](someEmail)
      case none =>
        eventsAndNextPage match {
          case (Nil, _)                           => EitherT.rightT[F, ProcessingRecoverableError](none)
          case (event :: eventsToCheck, nextPage) => matchEmailOnSingleCommit(event, project, eventsToCheck -> nextPage)
        }
    }

  private def matchEmailOnSingleCommit(event:         PushEvent,
                                       project:       Project,
                                       eventsToCheck: (List[PushEvent], Option[Int])
  )(implicit maybeAccessToken:                        Option[AccessToken]) =
    findCommitAuthor(project.path, event.commitId) >>= {
      case Some((event.authorName, email)) => EitherT.rightT[F, ProcessingRecoverableError](email.some)
      case _ => matchEmailFromCommits(eventsToCheck, project, EitherT.rightT[F, ProcessingRecoverableError](none))
    }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(List[PushEvent], Option[Int])]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage = response.headers.get(ci"X-Next-Page") >>= (_.head.value.toIntOption)
      response.as[List[PushEvent]].map(_ -> maybeNextPage)
    case (NotFound, _, _) => (List.empty[PushEvent] -> Option.empty[Int]).pure[F]
  }

  private implicit lazy val eventsDecoder: EntityDecoder[F, List[PushEvent]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import org.http4s.circe.jsonOf

    implicit val events: Decoder[Option[PushEvent]] = cursor =>
      for {
        projectId  <- cursor.downField("project_id").as[projects.Id]
        commitFrom <- cursor.downField("push_data").downField("commit_from").as[Option[CommitId]]
        commitTo   <- cursor.downField("push_data").downField("commit_to").as[Option[CommitId]]
        authorId   <- cursor.downField("author").downField("id").as[users.GitLabId]
        authorName <- cursor.downField("author").downField("name").as[users.Name]
      } yield (commitTo orElse commitFrom).map(PushEvent(projectId, _, authorId, authorName))

    jsonOf[F, List[Option[PushEvent]]].map(_.flatten)
  }

  private case class PushEvent(projectId:  projects.Id,
                               commitId:   CommitId,
                               authorId:   users.GitLabId,
                               authorName: users.Name
  )
}

private final case class Project(id: projects.Id, path: projects.Path)
