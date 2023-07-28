/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.creation

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import org.typelevel.log4cats.Logger

private trait ProjectSlugFinder[F[_]] {
  def findProjectSlug(projectId: projects.GitLabId, accessToken: AccessToken): OptionT[F, projects.Slug]
}

private class ProjectSlugFinderImpl[F[_]: Async: GitLabClient: Logger] extends ProjectSlugFinder[F] {

  import org.http4s.circe.jsonOf
  import cats.effect._
  import cats.syntax.all._
  import io.circe._
  import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
  import org.http4s._
  import org.http4s.implicits._

  def findProjectSlug(projectId: projects.GitLabId, accessToken: AccessToken): OptionT[F, projects.Slug] = OptionT {
    GitLabClient[F].get(uri"projects" / projectId.value, "single-project")(mapResponse)(accessToken.some)
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[projects.Slug]]] = {
    case (Ok, _, response)                           => response.as[projects.Slug].map(Option.apply)
    case (Unauthorized | Forbidden | NotFound, _, _) => Option.empty[projects.Slug].pure[F]
  }

  private implicit lazy val projectPathDecoder: EntityDecoder[F, projects.Slug] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    lazy val decoder: Decoder[projects.Slug] = _.downField("path_with_namespace").as[projects.Slug]
    jsonOf[F, projects.Slug](Sync[F], decoder)
  }
}

private object ProjectSlugFinder {

  def apply[F[_]: Async: GitLabClient: Logger]: F[ProjectSlugFinder[F]] =
    new ProjectSlugFinderImpl[F].pure[F].widen
}
