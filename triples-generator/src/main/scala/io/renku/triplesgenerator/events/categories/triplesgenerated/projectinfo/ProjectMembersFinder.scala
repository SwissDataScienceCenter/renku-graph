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

import cats.NonEmptyParallel
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.{GitLabApiUrl, projects, users}
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.http4s.Method.GET
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{NotFound, Ok}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

private trait ProjectMembersFinder[F[_]] {
  def findProjectMembers(path: projects.Path)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Set[ProjectMember]]
}

private object ProjectMembersFinder {
  def apply[F[_]: Async: NonEmptyParallel: Logger](gitLabThrottler: Throttler[F, GitLab]): F[ProjectMembersFinder[F]] =
    GitLabUrlLoader[F]().map(gitLabUrl => new ProjectMembersFinderImpl(gitLabUrl.apiV4, gitLabThrottler))
}

private class ProjectMembersFinderImpl[F[_]: Async: NonEmptyParallel: Logger](
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
    with ProjectMembersFinder[F] {

  import io.renku.tinytypes.json.TinyTypeDecoders._

  private type ProjectAndCreator = (GitLabProjectInfo, Option[users.GitLabId])

  override def findProjectMembers(path: projects.Path)(implicit
      maybeAccessToken:                 Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Set[ProjectMember]] = EitherT {
    (
      fetchMembers(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/members"),
      fetchMembers(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/users")
    ).parMapN(_ ++ _)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private implicit val memberDecoder: Decoder[ProjectMember] = cursor =>
    for {
      gitLabId <- cursor.downField("id").as[users.GitLabId]
      name     <- cursor.downField("name").as[users.Name]
      username <- cursor.downField("username").as[users.Username]
    } yield ProjectMember(name, username, gitLabId)

  private implicit lazy val memberEntityDecoder: EntityDecoder[F, ProjectMember]       = jsonOf[F, ProjectMember]
  private implicit lazy val membersDecoder:      EntityDecoder[F, List[ProjectMember]] = jsonOf[F, List[ProjectMember]]

  private def fetchMembers(
      url:                     String,
      maybePage:               Option[Int] = None,
      allMembers:              Set[ProjectMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[ProjectMember]] = for {
    uri                     <- validateUri(merge(url, maybePage))
    fetchedUsersAndNextPage <- send(secureRequest(GET, uri))(mapResponse)
    allMembers              <- addNextPage(url, allMembers, fetchedUsersAndNextPage)
  } yield allMembers

  private def merge(url: String, maybePage: Option[Int] = None) =
    maybePage map (page => s"$url?page=$page") getOrElse url

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(Set[ProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage: Option[Int] = response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)
      response.as[List[ProjectMember]].map(_.toSet -> maybeNextPage)
    case (NotFound, _, _) => (Set.empty[ProjectMember] -> Option.empty[Int]).pure[F]
  }

  private def addNextPage(
      url:                          String,
      allMembers:                   Set[ProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[ProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): F[Set[ProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetchMembers(url, maybeNextPage, allMembers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allMembers ++ fetchedUsers).pure[F]
    }
}
