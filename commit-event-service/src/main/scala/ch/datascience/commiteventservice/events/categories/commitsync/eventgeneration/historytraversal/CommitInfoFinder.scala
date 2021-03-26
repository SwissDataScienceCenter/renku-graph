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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitInfo
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Status}

import scala.concurrent.ExecutionContext

private trait CommitInfoFinder[Interpretation[_]] {
  def findCommitInfo(
      projectId:        Id,
      commitId:         CommitId,
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[CommitInfo]
}

private class CommitInfoFinderImpl(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with CommitInfoFinder[IO] {

  import CommitInfo._
  import cats.effect._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status.{Ok, Unauthorized}
  import org.http4s.{Request, Response}

  def findCommitInfo(projectId: Id, commitId: CommitId, maybeAccessToken: Option[AccessToken]): IO[CommitInfo] =
    for {
      uri    <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId")
      result <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield result

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[CommitInfo]] = {
    case (Ok, _, response)    => response.as[CommitInfo]
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit val commitInfoEntityDecoder: EntityDecoder[IO, CommitInfo] = jsonOf[IO, CommitInfo]
}
