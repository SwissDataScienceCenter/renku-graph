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

package io.renku.knowledgegraph.users.projects.finder

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.persons
import io.renku.http.client.{AccessToken, GitLabClient}
import org.typelevel.log4cats.Logger

private trait GLCreatorFinder[F[_]] {
  def findCreatorName(id: persons.GitLabId)(implicit maybeAccessToken: Option[AccessToken]): F[Option[persons.Name]]
}

private object GLCreatorFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GLCreatorFinder[F]] =
    new GLCreatorFinderImpl[F].pure[F].widen
}

private class GLCreatorFinderImpl[F[_]: Async: GitLabClient: Logger] extends GLCreatorFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._
  import org.http4s.implicits._
  import org.http4s.{EntityDecoder, Request, Response, Status}

  override def findCreatorName(id: persons.GitLabId)(implicit
      maybeAccessToken: Option[AccessToken]
  ): F[Option[persons.Name]] = GitLabClient[F].get(uri"users" / id, "single-user")(mapResponse)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[persons.Name]]] = {
    case (Ok, _, response) => response.as[Option[persons.Name]]
    case (NotFound, _, _)  => Option.empty[persons.Name].pure[F]
  }

  private implicit lazy val decoder: EntityDecoder[F, Option[persons.Name]] = {
    implicit val nameDecoder: Decoder[persons.Name] = Decoder.instance { cursor =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      cursor.downField("name").as(stringDecoder(persons.Name))
    }
    jsonOf[F, Option[persons.Name]]
  }
}
