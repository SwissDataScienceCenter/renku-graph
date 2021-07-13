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

package ch.datascience.commiteventservice.events.categories.common

import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.{AccessToken, RestClient}
import org.http4s.Status.NotFound
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[categories] trait CommitInfoFinder[Interpretation[_]] {
  def findCommitInfo(
      projectId: Id,
      commitId:  CommitId
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[CommitInfo]

  def getMaybeCommitInfo(projectId: Id, commitId: CommitId)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): Interpretation[Option[CommitInfo]]
}

private[categories] class CommitInfoFinderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[Interpretation, GitLab],
    logger:                  Logger[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RestClient(gitLabThrottler, logger)
    with CommitInfoFinder[Interpretation] {

  import CommitInfo._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status.{Ok, Unauthorized}
  import org.http4s.{Request, Response}

  def findCommitInfo(projectId: Id, commitId: CommitId)(implicit
      maybeAccessToken:         Option[AccessToken]
  ): Interpretation[CommitInfo] =
    fetchCommitInfo(projectId, commitId)(mapToCommitOrThrow)

  def getMaybeCommitInfo(projectId: Id, commitId: CommitId)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): Interpretation[Option[CommitInfo]] =
    fetchCommitInfo(projectId, commitId)(mapToMaybeCommit)

  private def fetchCommitInfo[ResultType](projectId: Id, commitId: CommitId)(
      mapResponse: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[
        ResultType
      ]]
  )(implicit maybeAccessToken: Option[AccessToken]) =
    for {
      uri    <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId")
      result <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield result

  private lazy val mapToCommitOrThrow
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[CommitInfo]] = {
    case (Ok, _, response)    => response.as[CommitInfo]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private lazy val mapToMaybeCommit: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]),
                                                     Interpretation[Option[CommitInfo]]
  ] = {
    case (Ok, _, response)    => response.as[CommitInfo].map(Some(_))
    case (NotFound, _, _)     => Option.empty[CommitInfo].pure[Interpretation]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitInfoEntityDecoder: EntityDecoder[Interpretation, CommitInfo] =
    jsonOf[Interpretation, CommitInfo]
}

private[categories] object CommitInfoFinder {
  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      concurrentEffect:      ConcurrentEffect[IO],
      timer:                 Timer[IO]
  ): IO[CommitInfoFinderImpl[IO]] = for {
    gitLabUrl <- GitLabUrl[IO]()
  } yield new CommitInfoFinderImpl[IO](gitLabUrl, gitLabThrottler, logger)
}
