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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import cats.data.OptionT
import cats.effect.{ConcurrentEffect, ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.{AccessToken, RestClient}
import org.typelevel.log4cats.Logger
import io.circe.Decoder
import io.circe.Decoder.decodeList
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Status}

import scala.concurrent.ExecutionContext
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.common.CommitInfo

private trait LatestCommitFinder[Interpretation[_]] {
  def findLatestCommit(
      projectId:        Id,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, CommitInfo]
}

private class LatestCommitFinderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[Interpretation, GitLab],
    logger:                  Logger[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RestClient(gitLabThrottler, logger)
    with LatestCommitFinder[Interpretation] {

  import CommitInfo._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status._
  import org.http4s.{Request, Response}

  override def findLatestCommit(
      projectId:        Id,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, CommitInfo] = OptionT {
    for {
      stringUri       <- s"$gitLabUrl/api/v4/projects/$projectId/repository/commits".pure[Interpretation]
      uri             <- validateUri(stringUri) map (_.withQueryParam("per_page", "1"))
      maybeCommitInfo <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield maybeCommitInfo
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]),
                                                Interpretation[Option[CommitInfo]]
  ] = {
    case (Ok, _, response)    => response.as[List[CommitInfo]] map (_.headOption)
    case (NotFound, _, _)     => Option.empty[CommitInfo].pure[Interpretation]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitInfosEntityDecoder: EntityDecoder[Interpretation, List[CommitInfo]] = {
    implicit val infosDecoder: Decoder[List[CommitInfo]] = decodeList[CommitInfo]
    jsonOf[Interpretation, List[CommitInfo]]
  }
}

private object LatestCommitFinder {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[LatestCommitFinder[IO]] = for {
    gitLabUrl <- GitLabUrl[IO]()
  } yield new LatestCommitFinderImpl(gitLabUrl, gitLabThrottler, logger)
}
