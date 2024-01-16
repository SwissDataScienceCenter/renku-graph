/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository
package deletion

import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.rest.paging.model.{Page, PerPage}

private trait RevokeCandidatesFinder[F[_]] {
  def projectAccessTokensStream(projectId: projects.GitLabId, accessToken: AccessToken): Stream[F, AccessTokenId]
}

private object RevokeCandidatesFinder {
  def apply[F[_]: Async: GitLabClient]: F[RevokeCandidatesFinder[F]] =
    RenkuAccessTokenName[F]().map(new RevokeCandidatesFinderImpl[F](PerPage(50), _))
}

private class RevokeCandidatesFinderImpl[F[_]: Async: GitLabClient](pageSize: PerPage,
                                                                    renkuTokenName: RenkuAccessTokenName
) extends RevokeCandidatesFinder[F] {

  import eu.timepit.refined.api.Refined
  import eu.timepit.refined.auto._
  import eu.timepit.refined.collection.NonEmpty
  import io.circe.Decoder
  import io.circe.Decoder._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
  import org.http4s.circe.jsonOf
  import org.http4s.implicits._
  import org.http4s.{EntityDecoder, Request, Response, Status}
  import org.typelevel.ci._

  private val endpointName: String Refined NonEmpty = "project-access-tokens"

  override def projectAccessTokensStream(projectId:   projects.GitLabId,
                                         accessToken: AccessToken
  ): Stream[F, AccessTokenId] =
    Stream
      .iterate(1)(_ + 1)
      .evalMap(fetch(_, projectId, accessToken))
      .takeThrough { case (_, maybeNextPage) => maybeNextPage.nonEmpty }
      .flatMap { case (tokens, _) => Stream.emits(tokens) }
      .filter { case (_, tokenName) => tokenName == renkuTokenName.value }
      .map { case (tokenId, _) => tokenId }

  private def fetch(page: Int, projectId: projects.GitLabId, accessToken: AccessToken) =
    GitLabClient[F]
      .get(uri"projects" / projectId / "access_tokens" +? ("per_page" -> pageSize) +? ("page" -> page), endpointName)(
        mapResponse
      )(accessToken.some)

  private type TokenInfo = (AccessTokenId, String)

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(List[TokenInfo], Option[Page])]] = {
    case (Ok, _, response)                           => response.as[List[TokenInfo]].map(_ -> maybeNextPage(response))
    case (Unauthorized | Forbidden | NotFound, _, _) => (List.empty[TokenInfo] -> Option.empty[Page]).pure[F]
  }

  private def maybeNextPage(response: Response[F]): Option[Page] =
    response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption.map(Page))

  private implicit lazy val decoder: EntityDecoder[F, List[TokenInfo]] = {

    implicit val itemDecoder: Decoder[TokenInfo] = cursor =>
      (cursor.downField("id").as[AccessTokenId], cursor.downField("name").as[String])
        .mapN((t, n) => (t, n))

    jsonOf[F, List[TokenInfo]]
  }
}
