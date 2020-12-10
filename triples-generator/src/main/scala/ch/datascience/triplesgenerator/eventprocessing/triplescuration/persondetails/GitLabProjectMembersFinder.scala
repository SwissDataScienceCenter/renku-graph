/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.{GitLabApiUrl, GitLabUrl}
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.http.client.RestClientError.ConnectivityException
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import org.http4s.Method.GET
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s.util.CaseInsensitiveString

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

private trait GitLabProjectMembersFinder[Interpretation[_]] {
  def findProjectMembers(path: Path)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, Set[GitLabProjectMember]]
}

private class IOGitLabProjectMembersFinder(
    gitLabApiUrl:    GitLabApiUrl,
    gitLabThrottler: Throttler[IO, GitLab],
    logger:          Logger[IO],
    retryInterval:   FiniteDuration = IORestClient.SleepAfterConnectionIssue
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(gitLabThrottler, logger, retryInterval = retryInterval)
    with GitLabProjectMembersFinder[IO] {

  override def findProjectMembers(
      path:                    Path
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, Set[GitLabProjectMember]] =
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
  ): EitherT[IO, ProcessingRecoverableError, Set[GitLabProjectMember]] = for {
    uri <- validateUri(merge(url, maybePage)).toRightT
    fetchedUsersAndNextPage <-
      EitherT(send(request(GET, uri, maybeAccessToken))(mapResponse) recoverWith maybeRecoverableError)
    allUsers <- addNextPage(url, allUsers, fetchedUsersAndNextPage)
  } yield allUsers

  private def merge(url: String, maybePage: Option[Int] = None) =
    maybePage map (page => s"$url?page=$page") getOrElse url

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[
    Either[ProcessingRecoverableError, (Set[GitLabProjectMember], Option[Int])]
  ]] = {
    case (Ok, _, response) =>
      response
        .as[List[GitLabProjectMember]]
        .map(members => Right(members.toSet -> maybeNextPage(response)))
    case (NotFound, _, _) =>
      Right(Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[IO]
    case (Forbidden | Unauthorized, _, _) =>
      Left(CurationRecoverableError("Access token not valid to fetch project members")).pure[IO]
  }

  private def addNextPage(
      url:                          String,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, Set[GitLabProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(url, maybeNextPage, allUsers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[IO].toRightT
    }

  private def maybeNextPage(response: Response[IO]): Option[Int] =
    response.headers.get(CaseInsensitiveString("X-Next-Page")).flatMap(_.value.toIntOption)

  private implicit lazy val projectDecoder: EntityDecoder[IO, List[GitLabProjectMember]] = {
    import ch.datascience.graph.model.users

    implicit val decoder: Decoder[GitLabProjectMember] = { cursor =>
      for {
        id       <- cursor.downField("id").as[GitLabId]
        username <- cursor.downField("username").as[users.Username]
        name     <- cursor.downField("name").as[users.Name]
      } yield GitLabProjectMember(id, username, name)
    }

    jsonOf[IO, List[GitLabProjectMember]]
  }

  private lazy val maybeRecoverableError
      : PartialFunction[Throwable, IO[Either[ProcessingRecoverableError, (Set[GitLabProjectMember], Option[Int])]]] = {
    case ConnectivityException(message, cause) =>
      IO.pure(Either.left(CurationRecoverableError(message, cause)))
  }

  private implicit class ResultOps[T](out: IO[T]) {
    lazy val toRightT: EitherT[IO, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](out)
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
