/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.IOContextShift
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ProjectHookCreator[Interpretation[_]] {
  def create(
      projectHook: ProjectHook,
      accessToken: AccessToken
  ): Interpretation[Unit]
}

private object ProjectHookCreator {

  final case class ProjectHook(
      projectId:           ProjectId,
      projectHookUrl:      ProjectHookUrl,
      serializedHookToken: SerializedHookToken
  )
}

@Singleton
private class IOProjectHookCreator @Inject()(
    gitLabConfigProvider:    IOGitLabConfigProvider
)(implicit executionContext: ExecutionContext, contextShift: IOContextShift)
    extends IORestClient
    with ProjectHookCreator[IO] {

  import cats.effect._
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import io.circe.Json
  import org.http4s.Method.POST
  import org.http4s.Status.{Created, Unauthorized}
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  def create(projectHook: ProjectHook, accessToken: AccessToken): IO[Unit] =
    for {
      gitLabUrl <- gitLabConfigProvider.get()
      uri       <- validateUri(s"$gitLabUrl/api/v4/projects/${projectHook.projectId}/hooks")
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

  private def mapResponse(request: Request[IO], response: Response[IO]): IO[Unit] =
    response.status match {
      case Created      => IO.pure(())
      case Unauthorized => IO.raiseError(UnauthorizedException)
      case _            => raiseError(request, response)
    }
}
