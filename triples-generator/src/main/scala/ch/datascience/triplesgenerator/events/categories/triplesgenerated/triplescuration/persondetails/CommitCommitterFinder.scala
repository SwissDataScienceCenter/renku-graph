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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger
import org.http4s.Method.GET
import org.http4s.Status.{Ok, Unauthorized}
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Request, Response, Status}

import scala.concurrent.ExecutionContext

private trait CommitCommitterFinder[Interpretation[_]] {

  def findCommitPeople(projectId:        projects.Id,
                       commitId:         CommitId,
                       maybeAccessToken: Option[AccessToken]
  ): Interpretation[CommitPersonsInfo]
}

private class CommitCommitterFinderImpl(
    gitLabApiUrl:    GitLabApiUrl,
    gitLabThrottler: Throttler[IO, GitLab],
    logger:          Logger[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends IORestClient(gitLabThrottler, logger)
    with CommitCommitterFinder[IO] {

  def findCommitPeople(projectId:        projects.Id,
                       commitId:         CommitId,
                       maybeAccessToken: Option[AccessToken]
  ): IO[CommitPersonsInfo] = for { // TODO: change this to EitherT. See GitLabProjectMembersFinder for an example
    uri    <- validateUri(s"$gitLabApiUrl/projects/$projectId/repository/commits/$commitId")
    result <- send(request(GET, uri, maybeAccessToken))(mapResponse)
  } yield result

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[CommitPersonsInfo]] = {
    case (Ok, _, response)    => response.as[CommitPersonsInfo]
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit val commitPersonInfoEntityDecoder: EntityDecoder[IO, CommitPersonsInfo] =
    jsonOf[IO, CommitPersonsInfo]

}

private object IOCommitCommitterFinder {
  def apply(gitLabApiUrl: GitLabApiUrl, gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:   ExecutionContext,
      contextShift:       ContextShift[IO],
      timer:              Timer[IO]
  ): IO[CommitCommitterFinder[IO]] = IO(new CommitCommitterFinderImpl(gitLabApiUrl, gitLabThrottler, logger))
}
