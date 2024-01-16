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

package io.renku.knowledgegraph.projects.update

import cats.effect.Async
import cats.effect.kernel.Concurrent
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.projects
import io.renku.http.client.{GitLabClient, UserAccessToken}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.{NotFound, Ok}
import org.http4s.circe.jsonOf
import org.http4s.implicits._
import org.http4s.{EntityDecoder, Request, Response, Status}

private trait ProjectGitUrlFinder[F[_]] {
  def findGitUrl(slug: projects.Slug, accessToken: UserAccessToken): F[Option[projects.GitHttpUrl]]
}

private object ProjectGitUrlFinder {
  def apply[F[_]: Async: GitLabClient]: ProjectGitUrlFinder[F] = new ProjectGitUrlFinderImpl[F]
}

private class ProjectGitUrlFinderImpl[F[_]: Async: GitLabClient] extends ProjectGitUrlFinder[F] {

  override def findGitUrl(slug: projects.Slug, at: UserAccessToken): F[Option[projects.GitHttpUrl]] =
    GitLabClient[F]
      .get(uri"projects" / slug, "single-project")(mapResponse)(at.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[projects.GitHttpUrl]]] = {
    case (Ok, _, resp)    => resp.as[Option[projects.GitHttpUrl]]
    case (NotFound, _, _) => Option.empty[projects.GitHttpUrl].pure[F]
  }

  private implicit lazy val entityDecoder: EntityDecoder[F, Option[projects.GitHttpUrl]] =
    jsonOf(implicitly[Concurrent[F]], Decoder.instance(_.downField("http_url_to_repo").as[Option[projects.GitHttpUrl]]))
}
