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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.{GitLabApiUrl, GitLabUrl}
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users.{GitLabId, Name}
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.client.{AccessToken, RestClient}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.Decoder
import org.http4s.Method.GET
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.util.CaseInsensitiveString
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private trait GitLabProjectMembersFinder[Interpretation[_]] {
  def findProjectMembers(path: Path)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): Interpretation[Set[GitLabProjectMember]]
}

private class IOGitLabProjectMembersFinder(
    gitLabApiUrl:    GitLabApiUrl,
    gitLabThrottler: Throttler[IO, GitLab],
    logger:          Logger[IO],
    retryInterval:   FiniteDuration = RestClient.SleepAfterConnectionIssue
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends RestClient(gitLabThrottler, logger, retryInterval = retryInterval)
    with GitLabProjectMembersFinder[IO] {

  override def findProjectMembers(
      path:                    Path
  )(implicit maybeAccessToken: Option[AccessToken]): IO[Set[GitLabProjectMember]] =
    for {
      users   <- fetch(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/users")
      members <- fetch(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/members")
    } yield users ++ members

  private def fetch(
      url:       String,
      maybePage: Option[Int] = None,
      allUsers:  Set[GitLabProjectMember] = Set.empty
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): IO[Set[GitLabProjectMember]] = for {
    uri                     <- merge(url, maybePage)
    fetchedUsersAndNextPage <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    allUsers                <- addNextPage(url, allUsers, fetchedUsersAndNextPage)
  } yield allUsers

  private def merge(url: String, maybePage: Option[Int] = None) = maybePage match {
    case Some(page) => validateUri(url).map(_.withQueryParam("page", page.toString))
    case None       => validateUri(url)
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[IO], Response[IO]), IO[(Set[GitLabProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      response
        .as[List[GitLabProjectMember]]
        .map(members => members.toSet -> maybeNextPage(response))
    case (NotFound, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[IO]

  }

  private def addNextPage(
      url:                          String,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): IO[Set[GitLabProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(url, maybeNextPage, allUsers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[IO]
    }

  private def maybeNextPage(response: Response[IO]): Option[Int] =
    response.headers.get(CaseInsensitiveString("X-Next-Page")).flatMap(_.value.toIntOption)

  private implicit lazy val projectDecoder: EntityDecoder[IO, List[GitLabProjectMember]] = {
    import ch.datascience.graph.model.users

    implicit val decoder: Decoder[GitLabProjectMember] = { cursor =>
      for {
        id   <- cursor.downField("id").as[GitLabId]
        name <- cursor.downField("name").as[users.Name]
      } yield GitLabProjectMember(id, name)
    }

    jsonOf[IO, List[GitLabProjectMember]]
  }
}

private object IOGitLabProjectMembersFinder {

  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO]
  ): IO[GitLabProjectMembersFinder[IO]] = for {
    gitLabUrl <- GitLabUrl[IO]()
  } yield new IOGitLabProjectMembersFinder(gitLabUrl.apiV4, gitLabThrottler, logger)
}

private final case class GitLabProjectMember(gitLabId: GitLabId, name: Name)
