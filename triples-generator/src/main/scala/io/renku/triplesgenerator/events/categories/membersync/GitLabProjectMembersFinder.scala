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

package io.renku.triplesgenerator.events.categories.membersync

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabApiUrl
import io.renku.graph.model.projects.Path
import io.renku.graph.model.users.{GitLabId, Name}
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Method.GET
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{Forbidden, NotFound, Ok, Unauthorized}
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

private trait GitLabProjectMembersFinder[F[_]] {
  def findProjectMembers(path: Path)(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]]
}

private class GitLabProjectMembersFinderImpl[F[_]: Async: Logger](
    gitLabApiUrl:    GitLabApiUrl,
    gitLabThrottler: Throttler[F, GitLab],
    retryInterval:   FiniteDuration = RestClient.SleepAfterConnectionIssue
) extends RestClient(gitLabThrottler, retryInterval = retryInterval)
    with GitLabProjectMembersFinder[F] {

  override def findProjectMembers(
      path:                    Path
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]] =
    for {
      users   <- fetch(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/users")
      members <- fetch(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/members")
    } yield users ++ members

  private def fetch(
      url:                     String,
      maybePage:               Option[Int] = None,
      allUsers:                Set[GitLabProjectMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]] = for {
    uri                     <- addPageToUrl(url, maybePage)
    fetchedUsersAndNextPage <- send(request(GET, uri, maybeAccessToken))(mapResponse(url))
    allUsers                <- addNextPage(url, allUsers, fetchedUsersAndNextPage)
  } yield allUsers

  private def addPageToUrl(url: String, maybePage: Option[Int] = None) = maybePage match {
    case Some(page) => validateUri(url).map(_.withQueryParam("page", page.toString))
    case None       => validateUri(url)
  }

  private def mapResponse(url: String)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): PartialFunction[(Status, Request[F], Response[F]), F[(Set[GitLabProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      response
        .as[List[GitLabProjectMember]]
        .map(members => members.toSet -> maybeNextPage(response))
    case (NotFound, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
    case (Unauthorized | Forbidden, _, _) =>
      maybeAccessToken match {
        case Some(_) => fetch(url)(maybeAccessToken = None).map(_ -> None)
        case None    => (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
      }
  }

  private def addNextPage(
      url:                          String,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): F[Set[GitLabProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(url, maybeNextPage, allUsers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[F]
    }

  private def maybeNextPage(response: Response[F]): Option[Int] =
    response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)

  private implicit lazy val projectDecoder: EntityDecoder[F, List[GitLabProjectMember]] = {
    import io.renku.graph.model.users

    implicit val decoder: Decoder[GitLabProjectMember] = { cursor =>
      for {
        id   <- cursor.downField("id").as[GitLabId]
        name <- cursor.downField("name").as[users.Name]
      } yield GitLabProjectMember(id, name)
    }

    jsonOf[F, List[GitLabProjectMember]]
  }
}

private object GitLabProjectMembersFinder {

  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[GitLabProjectMembersFinder[F]] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
  } yield new GitLabProjectMembersFinderImpl[F](gitLabUrl.apiV4, gitLabThrottler)
}

private final case class GitLabProjectMember(gitLabId: GitLabId, name: Name)
