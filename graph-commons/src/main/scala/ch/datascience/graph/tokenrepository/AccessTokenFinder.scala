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

package ch.datascience.graph.tokenrepository

import cats.effect.{ContextShift, IO}
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.{AccessToken, IORestClient}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait AccessTokenFinder[Interpretation[_]] {
  def findAccessToken(projectId: ProjectId): Interpretation[Option[AccessToken]]
}

class IOAccessTokenFinder(
    tokenRepositoryUrlProvider: TokenRepositoryUrlProvider[IO]
)(implicit executionContext:    ExecutionContext, contextShift: ContextShift[IO])
    extends IORestClient
    with AccessTokenFinder[IO] {

  import cats.effect._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  def findAccessToken(projectId: ProjectId): IO[Option[AccessToken]] =
    for {
      tokenRepositoryUrl <- tokenRepositoryUrlProvider.get
      uri                <- validateUri(s"$tokenRepositoryUrl/projects/$projectId/tokens")
      accessToken        <- send(request(GET, uri))(mapResponse)
    } yield accessToken

  private def mapResponse(request: Request[IO], response: Response[IO]): IO[Option[AccessToken]] =
    response.status match {
      case Ok       => response.as[Option[AccessToken]] handleErrorWith contextToError(request, response)
      case NotFound => IO.pure(None)
      case _        => raiseError(request, response)
    }

  private implicit lazy val accessTokenEntityDecoder: EntityDecoder[IO, Option[AccessToken]] = {
    jsonOf[IO, AccessToken].map(Option.apply)
  }
}
