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

package io.renku.webhookservice.tokenrepository

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.graph.model.projects.Id
import io.renku.graph.tokenrepository.TokenRepositoryUrl
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.Status
import org.typelevel.log4cats.Logger

trait AccessTokenAssociator[F[_]] {
  def associate(projectId: Id, accessToken: AccessToken): F[Unit]
}

class AccessTokenAssociatorImpl[F[_]: Async: Logger](
    tokenRepositoryUrl: TokenRepositoryUrl
) extends RestClient[F, AccessTokenAssociator[F]](Throttler.noThrottling)
    with AccessTokenAssociator[F] {

  import io.circe.syntax._
  import org.http4s.Method.PUT
  import org.http4s.Status.NoContent
  import org.http4s.circe._
  import org.http4s.{Request, Response}

  override def associate(projectId: Id, accessToken: AccessToken): F[Unit] =
    for {
      uri <- validateUri(s"$tokenRepositoryUrl/projects/$projectId/tokens")
      requestWithPayload = request(PUT, uri).withEntity(accessToken.asJson)
      _ <- send(requestWithPayload)(mapResponse)
    } yield ()

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Unit]] = {
    case (NoContent, _, _) => ().pure[F]
  }
}

object AccessTokenAssociator {
  def apply[F[_]: Async: Logger]: F[AccessTokenAssociator[F]] =
    for {
      tokenRepositoryUrl <- TokenRepositoryUrl[F]()
    } yield new AccessTokenAssociatorImpl[F](tokenRepositoryUrl)
}
