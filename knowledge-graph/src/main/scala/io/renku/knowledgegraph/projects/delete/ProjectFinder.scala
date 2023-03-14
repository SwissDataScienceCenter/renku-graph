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

package io.renku.knowledgegraph.projects.delete

import cats.effect.Async
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}

private trait ProjectFinder[F[_]] {
  def findProject(path: projects.Path)(implicit at: AccessToken): F[Option[Project]]
}

private object ProjectFinder {
  def apply[F[_]: Async: GitLabClient]: ProjectFinder[F] = new ProjectFinderImpl[F]
}

private class ProjectFinderImpl[F[_]: Async: GitLabClient] extends ProjectFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s._
  import org.http4s.Status._
  import org.http4s.circe._
  import org.http4s.implicits._

  override def findProject(path: projects.Path)(implicit at: AccessToken): F[Option[Project]] =
    GitLabClient[F].get(uri"projects" / path, "single-project")(mapResponse)(at.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[Project]]] = {
    case (Ok, _, response) => response.as[Project].map(Option.apply)
    case (NotFound, _, _)  => Option.empty[Project].pure[F]
  }

  private implicit lazy val decoder: Decoder[Project] = Decoder.instance { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._
    (cursor.downField("id").as[projects.GitLabId], cursor.downField("path_with_namespace").as[projects.Path])
      .mapN(Project(_, _))
  }

  private implicit lazy val eDecoder: EntityDecoder[F, Project] = jsonOf
}
