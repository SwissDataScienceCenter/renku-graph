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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.tokenrepository.api.TokenRepositoryClient
import org.typelevel.log4cats.Logger

import java.time.Instant

private trait GLDataFinder[F[_]] {
  def fetchGLData(slug: projects.Slug): F[Option[DataExtract.GL]]
}

private object GLDataFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GLDataFinder[F]] =
    TokenRepositoryClient[F].map(new GLDataFinderImpl[F](_))
}

private class GLDataFinderImpl[F[_]: Async: GitLabClient](trClient: TokenRepositoryClient[F]) extends GLDataFinder[F] {

  import trClient.findAccessToken
  import cats.data.OptionT
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status._
  import org.http4s._
  import org.http4s.circe.CirceEntityDecoder._
  import org.http4s.implicits._

  override def fetchGLData(slug: projects.Slug): F[Option[DataExtract.GL]] =
    OptionT(findAccessToken(slug))
      .flatMapF(at => GitLabClient[F].get(uri"projects" / slug, "single-project")(mapResponse)(at.some))
      .value

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[DataExtract.GL]]] = {
    case (Ok, _, response)                           => response.as[DataExtract.GL].map(Option.apply)
    case (Unauthorized | Forbidden | NotFound, _, _) => Option.empty[DataExtract.GL].pure[F]
  }

  private implicit lazy val decoder: Decoder[DataExtract.GL] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    (cursor.downField("path_with_namespace").as[projects.Slug],
     cursor.downField("name").as[projects.Name],
     cursor.downField("visibility").as[projects.Visibility],
     cursor.downField("updated_at").as[Option[Instant]],
     cursor.downField("last_activity_at").as[Option[Instant]],
     cursor.downField("description").as[Option[projects.Description]],
     cursor.downField("topics").as[Set[Option[projects.Keyword]]].map(_.flatten),
     cursor.downField("avatar_url").as[Option[ImageUri]]
    ).mapN(DataExtract.GL.apply)
  }
}
