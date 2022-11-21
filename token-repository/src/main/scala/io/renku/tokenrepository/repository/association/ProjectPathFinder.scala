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

package io.renku.tokenrepository.repository.association

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import org.typelevel.log4cats.Logger

trait ProjectPathFinder[F[_]] {
  def findProjectPath(projectId: projects.Id, accessToken: AccessToken): OptionT[F, projects.Path]
}

private class ProjectPathFinderImpl[F[_]: Async: GitLabClient: Logger] extends ProjectPathFinder[F] {

  import org.http4s.circe.jsonOf
  import cats.effect._
  import cats.syntax.all._
  import io.circe._
  import org.http4s.Status.{NotFound, Ok, Unauthorized}
  import org.http4s._
  import org.http4s.implicits._

  def findProjectPath(projectId: projects.Id, accessToken: AccessToken): OptionT[F, projects.Path] = OptionT {
    GitLabClient[F].get(uri"projects" / projectId.value, "single-project")(mapResponse)(accessToken.some)
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[projects.Path]]] = {
    case (Ok, _, response)    => response.as[projects.Path].map(Option.apply)
    case (NotFound, _, _)     => Option.empty[projects.Path].pure[F]
    case (Unauthorized, _, _) => Option.empty[projects.Path].pure[F]
  }

  private implicit lazy val projectPathDecoder: EntityDecoder[F, projects.Path] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    lazy val decoder: Decoder[projects.Path] = _.downField("path_with_namespace").as[projects.Path]
    jsonOf[F, projects.Path](Sync[F], decoder)
  }
}

object ProjectPathFinder {

  def apply[F[_]: Async: GitLabClient: Logger]: F[ProjectPathFinder[F]] =
    new ProjectPathFinderImpl[F].pure[F].widen
}
