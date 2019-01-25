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
import ch.datascience.clients.{AccessToken, IORestClient}
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.model.{HookAccessToken, ProjectInfo}
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait HookAccessTokenCreator[Interpretation[_]] {
  def createHookAccessToken(
      projectInfo: ProjectInfo,
      accessToken: AccessToken
  ): Interpretation[HookAccessToken]
}

@Singleton
private class IOHookAccessTokenCreator @Inject()(gitLabConfigProvider: IOGitLabConfigProvider)(
    implicit executionContext:                                         ExecutionContext)
    extends IORestClient
    with HookAccessTokenCreator[IO] {

  import cats.effect._
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import io.circe._
  import org.http4s.Method.POST
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  def createHookAccessToken(projectInfo: ProjectInfo, accessToken: AccessToken): IO[HookAccessToken] =
    for {
      gitLabHostUrl <- gitLabConfigProvider.get()
      uri           <- validateUri(s"$gitLabHostUrl/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
      projectInfo   <- send(postRequest(uri, accessToken, projectInfo))(mapResponse)
    } yield projectInfo

  private def postRequest(uri: Uri, accessToken: AccessToken, projectInfo: ProjectInfo): Request[IO] =
    request(POST, uri, accessToken)
      .withEntity {
        UrlForm(
          "user_id"  -> projectInfo.owner.id.toString,
          "name"     -> s"${projectInfo.path.value.replace("/", "-")}-hook-access-token",
          "scopes[]" -> "api"
        )
      }

  private def mapResponse(request: Request[IO], response: Response[IO]): IO[HookAccessToken] =
    response.status match {
      case Created      => response.as[HookAccessToken] handleErrorWith contextToError(request, response)
      case Unauthorized => F.raiseError(UnauthorizedException)
      case _            => raiseError(request, response)
    }

  private implicit lazy val hookAccessTokenDecoder: EntityDecoder[IO, HookAccessToken] = {
    implicit val hookAccessTokenDecoder: Decoder[HookAccessToken] = (cursor: HCursor) =>
      cursor
        .downField("token")
        .as[String]
        .map(HookAccessToken.apply)

    jsonOf[IO, HookAccessToken]
  }
}
