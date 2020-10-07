/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.tokenrepository

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.TokenRepositoryUrl
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger
import org.http4s.Status

import scala.concurrent.ExecutionContext

trait AccessTokenAssociator[Interpretation[_]] {
  def associate(projectId: Id, accessToken: AccessToken): Interpretation[Unit]
}

class IOAccessTokenAssociator(
    tokenRepositoryUrl:      TokenRepositoryUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(Throttler.noThrottling, logger)
    with AccessTokenAssociator[IO] {

  import cats.effect._
  import io.circe.syntax._
  import org.http4s.Method.PUT
  import org.http4s.Status.NoContent
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  override def associate(projectId: Id, accessToken: AccessToken): IO[Unit] =
    for {
      uri <- validateUri(s"$tokenRepositoryUrl/projects/$projectId/tokens")
      requestWithPayload = request(PUT, uri).withEntity(accessToken.asJson)
      _ <- send(requestWithPayload)(mapResponse)
    } yield ()

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (NoContent, _, _) => IO.unit
  }
}

object IOAccessTokenAssociator {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[AccessTokenAssociator[IO]] =
    for {
      tokenRepositoryUrl <- TokenRepositoryUrl[IO]()
    } yield new IOAccessTokenAssociator(tokenRepositoryUrl, logger)
}
