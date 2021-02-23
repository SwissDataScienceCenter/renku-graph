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
  ): Interpretation[CommitPersonInfo]
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
  ): IO[CommitPersonInfo] = for {
    uri    <- validateUri(s"$gitLabApiUrl/projects/$projectId/repository/commits/$commitId")
    result <- send(request(GET, uri, maybeAccessToken))(mapResponse)
  } yield result

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[CommitPersonInfo]] = {
    case (Ok, _, response)    => response.as[CommitPersonInfo]
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit val commitPersonInfoEntityDecoder: EntityDecoder[IO, CommitPersonInfo] = jsonOf[IO, CommitPersonInfo]

  """
    |{
    |  "id": "6104942438c14ec7bd21c6cd5bd995272b3faff6",
    |  "short_id": "6104942438c",
    |  "title": "Sanitize for network graph",
    |  "author_name": "randx",
    |  "author_email": "user@example.com",
    |  "committer_name": "Dmitriy",
    |  "committer_email": "user@example.com",
    |  "created_at": "2012-09-20T09:06:12+03:00",
    |  "message": "Sanitize for network graph",
    |  "committed_date": "2012-09-20T09:06:12+03:00",
    |  "authored_date": "2012-09-20T09:06:12+03:00",
    |  "parent_ids": [
    |    "ae1d9fb46aa2b07ee9836d49862ec4e2c46fbbba"
    |  ],
    |  "last_pipeline" : {
    |    "id": 8,
    |    "ref": "master",
    |    "sha": "2dc6aa325a317eda67812f05600bdf0fcdc70ab0",
    |    "status": "created"
    |  },
    |  "stats": {
    |    "additions": 15,
    |    "deletions": 10,
    |    "total": 25
    |  },
    |  "status": "running",
    |  "web_url": "https://gitlab.example.com/thedude/gitlab-foss/-/commit/6104942438c14ec7bd21c6cd5bd995272b3faff6"
    |}
    |""".stripMargin

}

private object IOCommitCommitterFinder {
  def apply(gitLabApiUrl: GitLabApiUrl, gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:   ExecutionContext,
      contextShift:       ContextShift[IO],
      timer:              Timer[IO]
  ): IO[CommitCommitterFinder[IO]] = IO(new CommitCommitterFinderImpl(gitLabApiUrl, gitLabThrottler, logger))
}
