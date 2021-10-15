/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import ProjectHookCreator.ProjectHook
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrlLoader
import ch.datascience.graph.model.GitLabUrl
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.{AccessToken, RestClient}
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Status
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait ProjectHookCreator[Interpretation[_]] {
  def create(
      projectHook: ProjectHook,
      accessToken: AccessToken
  ): Interpretation[Unit]
}

private object ProjectHookCreator {

  final case class ProjectHook(
      projectId:           Id,
      projectHookUrl:      ProjectHookUrl,
      serializedHookToken: SerializedHookToken
  )
}

private class IOProjectHookCreator(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RestClient(gitLabThrottler, logger)
    with ProjectHookCreator[IO] {

  import cats.effect._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import io.circe.Json
  import org.http4s.Method.POST
  import org.http4s.Status.{Created, Unauthorized}
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  def create(projectHook: ProjectHook, accessToken: AccessToken): IO[Unit] =
    for {
      uri <- validateUri(s"$gitLabUrl/api/v4/projects/${projectHook.projectId}/hooks")
      requestWithPayload = request(POST, uri, accessToken).withEntity(payload(projectHook))
      result <- send(requestWithPayload)(mapResponse)
    } yield result

  private def payload(projectHook: ProjectHook) =
    Json.obj(
      "id"          -> Json.fromInt(projectHook.projectId.value),
      "url"         -> Json.fromString(projectHook.projectHookUrl.value),
      "push_events" -> Json.fromBoolean(true),
      "token"       -> Json.fromString(projectHook.serializedHookToken.value)
    )

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Created, _, _)      => IO.unit
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }
}

private object IOProjectHookCreator {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectHookCreator[IO]] =
    for {
      gitLabUrl <- GitLabUrlLoader[IO]()
    } yield new IOProjectHookCreator(gitLabUrl, gitLabThrottler, logger)
}
