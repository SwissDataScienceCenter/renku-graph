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
import ch.datascience.graph.events._
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.model.AccessToken
import ch.datascience.webhookservice.model.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import io.circe.Decoder.decodeList
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private abstract class GraphTokenVerifier[Interpretation[_]] {
  def checkTokenPresence(
      projectId:   ProjectId,
      userId:      UserId,
      accessToken: AccessToken
  ): Interpretation[Boolean]
}

@Singleton
private class IOGraphTokenVerifier @Inject()(gitLabConfigProvider: IOGitLabConfigProvider)(
    implicit executionContext:                                     ExecutionContext)
    extends GraphTokenVerifier[IO] {

  import cats.effect._
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import io.circe._
  import org.http4s.AuthScheme.Bearer
  import org.http4s.Credentials.Token
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s.circe._
  import org.http4s.client.blaze.BlazeClientBuilder
  import org.http4s.dsl.io._
  import org.http4s.headers.Authorization
  import org.http4s._

  private implicit val cs: ContextShift[IO] = IO.contextShift(executionContext)
  private val F = implicitly[ConcurrentEffect[IO]]

  def checkTokenPresence(projectId: ProjectId, userId: UserId, accessToken: AccessToken): IO[Boolean] =
    for {
      gitLabHostUrl <- gitLabConfigProvider.get()
      uri           <- F.fromEither(Uri.fromString(s"$gitLabHostUrl/api/v4/users/$userId/impersonation_tokens"))
      existingHooks <- send(request(uri, accessToken))
    } yield checkProjectHookExists(existingHooks, projectId)

  private def request(uri: Uri, accessToken: AccessToken) =
    Request[IO](
      method  = GET,
      uri     = uri,
      headers = authHeader(accessToken)
    )

  private lazy val authHeader: AccessToken => Headers = {
    case PersonalAccessToken(token) => Headers(Header("PRIVATE-TOKEN", token))
    case OAuthAccessToken(token)    => Headers(Authorization(Token(Bearer, token)))
  }

  private def send(request: Request[IO]) = BlazeClientBuilder[IO](executionContext).resource.use { httpClient =>
    httpClient.fetch[List[String]](request) { response =>
      response.status match {
        case Ok           => response.as[List[String]]
        case Unauthorized => F.raiseError(UnauthorizedException)
        case _            => raiseError(request, response)
      }
    }
  }

  private def raiseError(request: Request[IO], response: Response[IO]): IO[List[String]] =
    response
      .as[String]
      .flatMap { bodyAsString =>
        F.raiseError {
          new RuntimeException(
            s"${request.method} ${request.uri} returned ${response.status}; body: ${bodyAsString.split('\n').map(_.trim.filter(_ >= ' ')).mkString}"
          )
        }
      }

  private implicit lazy val hooksNamesDecoder: EntityDecoder[IO, List[String]] = {
    implicit val hookNameDecoder: Decoder[List[String]] = decodeList {
      _.downField("name").as[String]
    }

    jsonOf[IO, List[String]]
  }

  private def checkProjectHookExists(hookNames: List[String], projectId: ProjectId): Boolean = {
    println(hookNames.mkString("; "))
    hookNames.contains(s"renku-graph-$projectId")
  }
}
