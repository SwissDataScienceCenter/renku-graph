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

package io.renku.knowledgegraph.projects.update

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, Json}
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status._
import org.http4s.circe.jsonOf
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, UrlForm}

private trait GLProjectUpdater[F[_]] {
  def updateProject(slug: projects.Slug, updates: ProjectUpdates, at: AccessToken): EitherT[F, Json, Unit]
}

private object GLProjectUpdater {
  def apply[F[_]: Async: GitLabClient]: GLProjectUpdater[F] = new GLProjectUpdaterImpl[F]
}

private class GLProjectUpdaterImpl[F[_]: Async: GitLabClient] extends GLProjectUpdater[F] {

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates, at: AccessToken): EitherT[F, Json, Unit] =
    EitherT {
      GitLabClient[F].put(uri"projects" / slug, "edit-project", UrlForm("visibility" -> updates.visibility.value))(
        mapResponse
      )(at.some)
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Either[Json, Unit]]] = {
    case (Ok, _, _)                => ().asRight[Json].pure[F]
    case (BadRequest, _, response) => response.as[Json](MonadThrow[F], jsonOf(Async[F], errorDecoder)).map(_.asLeft)
  }

  private lazy val errorDecoder: Decoder[Json] = { cur =>
    (cur.downField("error").as[Option[Json]], cur.downField("message").as[Option[Json]])
      .mapN(_ orElse _ getOrElse cur.value)
  }
}
