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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.effect.{Async, Sync}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status._
import org.http4s.circe.jsonOf
import org.http4s.implicits._
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.log4cats.Logger

private trait GLProjectVisibilityFinder[F[_]] {
  def findVisibility(slug: projects.Slug)(implicit at: AccessToken): F[Option[projects.Visibility]]
}

private object GLProjectVisibilityFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: GLProjectVisibilityFinder[F] = new GLProjectVisibilityFinderImpl[F]
}

private class GLProjectVisibilityFinderImpl[F[_]: Async: GitLabClient] extends GLProjectVisibilityFinder[F] {

  override def findVisibility(slug: projects.Slug)(implicit at: AccessToken): F[Option[projects.Visibility]] =
    GitLabClient[F].get(uri"projects" / slug, "single-project")(mapResponse)(at.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[projects.Visibility]]] = {
    case (Ok, _, response)                           => response.as[Option[projects.Visibility]]
    case (Unauthorized | Forbidden | NotFound, _, _) => Option.empty[projects.Visibility].pure[F]
  }

  private implicit lazy val entityDecoder: EntityDecoder[F, Option[projects.Visibility]] = {
    lazy val decoder: Decoder[Option[projects.Visibility]] = _.downField("visibility").as[Option[projects.Visibility]]
    jsonOf[F, Option[projects.Visibility]](Sync[F], decoder)
  }
}
