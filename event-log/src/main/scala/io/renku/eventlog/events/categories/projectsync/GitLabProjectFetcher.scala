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

package io.renku.eventlog.events.categories.projectsync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.http.client.RestClientError.UnauthorizedException
import org.http4s.Status.{Forbidden, InternalServerError, NotFound, Ok, Unauthorized}
import org.http4s.{EntityDecoder, Request, Response, Status}

private trait GitLabProjectFetcher[F[_]] {
  def fetchGitLabProject(projectId: projects.Id): F[Either[UnauthorizedException, Option[projects.Path]]]
}

private object GitLabProjectFetcher {
  def apply[F[_]: Async: GitLabClient: AccessTokenFinder]: F[GitLabProjectFetcher[F]] =
    MonadThrow[F].catchNonFatal(new GitLabProjectFetcherImpl[F]).widen
}

private class GitLabProjectFetcherImpl[F[_]: Async: GitLabClient: AccessTokenFinder] extends GitLabProjectFetcher[F] {

  private val tokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import org.http4s.implicits._
  import tokenFinder._

  override def fetchGitLabProject(projectId: projects.Id): F[Either[UnauthorizedException, Option[projects.Path]]] =
    findAccessToken(projectId) >>= { implicit accessToken =>
      GitLabClient[F].get[Either[UnauthorizedException, Option[projects.Path]]](uri"projects" / projectId.show,
                                                                                "single-project"
      )(mapping)
    }

  private lazy val mapping
      : PartialFunction[(Status, Request[F], Response[F]), F[Either[UnauthorizedException, Option[projects.Path]]]] = {
    case (Ok, _, response) => response.as[Option[projects.Path]].map(_.asRight)
    case (NotFound | InternalServerError, _, _) =>
      Option.empty[projects.Path].asRight[UnauthorizedException].pure[F]
    case (Unauthorized | Forbidden, _, _) =>
      UnauthorizedException.asLeft[Option[projects.Path]].pure[F]
  }

  private implicit val entityDecoder: EntityDecoder[F, Option[projects.Path]] = {
    import org.http4s.circe.jsonOf

    implicit val decoder: Decoder[Option[projects.Path]] = {
      import io.circe.Decoder.decodeOption
      import io.renku.tinytypes.json.TinyTypeDecoders.relativePathDecoder
      _.downField("path_with_namespace").as(decodeOption(relativePathDecoder(projects.Path)))
    }
    jsonOf[F, Option[projects.Path]]
  }
}
