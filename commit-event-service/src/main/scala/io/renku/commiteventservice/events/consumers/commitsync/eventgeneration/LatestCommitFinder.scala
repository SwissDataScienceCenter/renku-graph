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

package io.renku.commiteventservice.events.consumers.commitsync.eventgeneration

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.commiteventservice.events.consumers.common.CommitInfo
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, GitLabClient}
import org.http4s.circe.jsonOf
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{EntityDecoder, Status}
import org.typelevel.log4cats.Logger

private trait LatestCommitFinder[F[_]] {
  def findLatestCommit(projectId: Id)(implicit maybeAccessToken: Option[AccessToken]): F[Option[CommitInfo]]
}

private class LatestCommitFinderImpl[F[_]: Async: GitLabClient: Logger] extends LatestCommitFinder[F] {

  import CommitInfo._
  import org.http4s.Status._
  import org.http4s.{Request, Response}

  override def findLatestCommit(projectId: Id)(implicit maybeAccessToken: Option[AccessToken]): F[Option[CommitInfo]] =
    GitLabClient[F].get(uri"projects" / projectId.show / "repository" / "commits" withQueryParam ("per_page", "1"),
                        "commits"
    )(mapResponse(projectId))

  private def mapResponse(projectId: Id): PartialFunction[(Status, Request[F], Response[F]), F[Option[CommitInfo]]] = {
    case (Ok, _, response)                      => response.as[List[CommitInfo]] map (_.headOption)
    case (NotFound | InternalServerError, _, _) => Option.empty[CommitInfo].pure[F]
    case (Unauthorized | Forbidden, _, _)       => findLatestCommit(projectId)(maybeAccessToken = None)
  }

  private implicit val commitInfosEntityDecoder: EntityDecoder[F, List[CommitInfo]] = {
    implicit val infosDecoder: Decoder[List[CommitInfo]] = decodeList[CommitInfo]
    jsonOf[F, List[CommitInfo]]
  }
}

private object LatestCommitFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[LatestCommitFinder[F]] =
    new LatestCommitFinderImpl[F].pure[F].widen
}
