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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Status}
import org.typelevel.log4cats.Logger

private trait LatestCommitFinder[F[_]] {
  def findLatestCommit(projectId: Id)(implicit maybeAccessToken: Option[AccessToken]): F[Option[CommitInfo]]
}

private class LatestCommitFinderImpl[F[_]: Async: Logger](
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with LatestCommitFinder[F] {

  import CommitInfo._
  import org.http4s.Method.GET
  import org.http4s.Status._
  import org.http4s.{Request, Response}

  override def findLatestCommit(projectId: Id)(implicit maybeAccessToken: Option[AccessToken]): F[Option[CommitInfo]] =
    for {
      stringUri       <- s"$gitLabUrl/api/v4/projects/$projectId/repository/commits".pure[F]
      uri             <- validateUri(stringUri) map (_.withQueryParam("per_page", "1"))
      maybeCommitInfo <- send(secureRequest(GET, uri))(mapResponse(projectId))
    } yield maybeCommitInfo

  private def mapResponse(projectId: Id): PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitInfo]]] = {
    case (Ok, _, response)    => response.as[List[CommitInfo]] map (_.headOption)
    case (NotFound, _, _)     => Option.empty[CommitInfo].pure[F]
    case (Unauthorized, _, _) => findLatestCommit(projectId)(maybeAccessToken = None)
  }

  private implicit val commitInfosEntityDecoder: EntityDecoder[F, List[CommitInfo]] = {
    implicit val infosDecoder: Decoder[List[CommitInfo]] = decodeList[CommitInfo]
    jsonOf[F, List[CommitInfo]]
  }
}

private object LatestCommitFinder {
  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[LatestCommitFinder[F]] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
  } yield new LatestCommitFinderImpl[F](gitLabUrl, gitLabThrottler)
}
