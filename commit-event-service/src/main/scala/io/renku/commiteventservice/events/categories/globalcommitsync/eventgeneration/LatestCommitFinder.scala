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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import cats.data.OptionT
import cats.effect.{Async, Temporal}
import cats.syntax.all._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Status}
import org.typelevel.log4cats.Logger

private trait LatestCommitFinder[Interpretation[_]] {
  def findLatestCommitId(
      projectId:        Id,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, CommitId]
}

private class LatestCommitFinderImpl[Interpretation[_]: Async: Temporal: Logger](
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[Interpretation, GitLab]
) extends RestClient(gitLabThrottler)
    with LatestCommitFinder[Interpretation] {

  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status._
  import org.http4s.{Request, Response}

  override def findLatestCommitId(
      projectId:        Id,
      maybeAccessToken: Option[AccessToken]
  ): OptionT[Interpretation, CommitId] = OptionT {
    for {
      stringUri     <- s"$gitLabUrl/api/v4/projects/$projectId/repository/commits".pure[Interpretation]
      uri           <- validateUri(stringUri) map (_.withQueryParam("per_page", "1"))
      maybeCommitId <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield maybeCommitId
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[Interpretation], Response[Interpretation]),
                                                Interpretation[Option[CommitId]]
  ] = {
    case (Ok, _, response)    => response.as[List[CommitInfo]] map (_.headOption.map(_.id))
    case (NotFound, _, _)     => Option.empty[CommitId].pure[Interpretation]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError
  }

  private implicit val commitInfosEntityDecoder: EntityDecoder[Interpretation, List[CommitInfo]] = {
    implicit val infosDecoder: Decoder[List[CommitInfo]] = decodeList[CommitInfo]
    jsonOf[Interpretation, List[CommitInfo]]
  }
}

private object LatestCommitFinder {
  def apply[Interpretation[_]: Async: Temporal: Logger](
      gitLabThrottler: Throttler[Interpretation, GitLab]
  ): Interpretation[LatestCommitFinder[Interpretation]] = for {
    gitLabUrl <- GitLabUrlLoader[Interpretation]()
  } yield new LatestCommitFinderImpl(gitLabUrl, gitLabThrottler)
}
