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

package io.renku.webhookservice.hookcreation

import cats.Applicative
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Status
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private trait ProjectHookCreator[F[_]] {
  def create(projectHook: ProjectHook, accessToken: AccessToken): F[Unit]
}

private class ProjectHookCreatorImpl[F[_]: Async: GitLabClient: Logger] extends ProjectHookCreator[F] {

  import cats.effect._
  import io.circe.Json
  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s.Status.{Created, Unauthorized}
  import org.http4s.{Request, Response}

  def create(projectHook: ProjectHook, accessToken: AccessToken): F[Unit] = {
    val uri = uri"projects" / projectHook.projectId.show / "hooks"
    GitLabClient[F].post(uri, "create-hook", payload(projectHook))(mapResponse)(Some(accessToken))
  }

  private def payload(projectHook: ProjectHook) = Json.obj(
    "id"          -> Json.fromInt(projectHook.projectId.value),
    "url"         -> Json.fromString(projectHook.projectHookUrl.value),
    "push_events" -> Json.fromBoolean(true),
    "token"       -> Json.fromString(projectHook.serializedHookToken.value)
  )

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = {
    case (Created, _, _)      => Applicative[F].unit
    case (Unauthorized, _, _) => MonadCancelThrow[F].raiseError(UnauthorizedException)
  }
}

private object ProjectHookCreator {
  final case class ProjectHook(
      projectId:           GitLabId,
      projectHookUrl:      ProjectHookUrl,
      serializedHookToken: SerializedHookToken
  )

  def apply[F[_]: Async: GitLabClient: Logger]: F[ProjectHookCreator[F]] =
    new ProjectHookCreatorImpl[F].pure[F].widen[ProjectHookCreator[F]]
}
