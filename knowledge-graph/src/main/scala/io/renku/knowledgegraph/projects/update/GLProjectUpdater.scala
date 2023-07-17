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
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status._
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, UrlForm}

private trait GLProjectUpdater[F[_]] {
  def updateProject(path: projects.Path, newValues: NewValues, at: AccessToken): F[Unit]
}

private object GLProjectUpdater {
  def apply[F[_]: MonadThrow: GitLabClient]: GLProjectUpdater[F] = new GLProjectUpdaterImpl[F]
}

private class GLProjectUpdaterImpl[F[_]: MonadThrow: GitLabClient] extends GLProjectUpdater[F] {

  override def updateProject(path: projects.Path, newValues: NewValues, at: AccessToken): F[Unit] =
    GitLabClient[F].put(uri"projects" / path, "edit-project", UrlForm("visibility" -> newValues.visibility.value))(
      mapResponse
    )(at.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = { case (Ok, _, _) =>
    ().pure[F]
  }
}
