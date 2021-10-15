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

import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.TokenRepositoryUrl
import ch.datascience.http.client.RestClient
import org.http4s.Status
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

trait AccessTokenRemover[Interpretation[_]] {
  def removeAccessToken(projectId: Id): Interpretation[Unit]
}

class AccessTokenRemoverImpl[Interpretation[_]: ConcurrentEffect: Timer](
    tokenRepositoryUrl:      TokenRepositoryUrl,
    logger:                  Logger[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, AccessTokenRemover[Interpretation]](Throttler.noThrottling, logger)
    with AccessTokenRemover[Interpretation] {

  import org.http4s.Method.DELETE
  import org.http4s.Status.NoContent
  import org.http4s.{Request, Response}

  override def removeAccessToken(projectId: Id): Interpretation[Unit] =
    for {
      uri <- validateUri(s"$tokenRepositoryUrl/projects/$projectId/tokens")
      _   <- send(request(DELETE, uri))(mapResponse)
    } yield ()

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (NoContent, _, _) => ().pure[Interpretation]
  }
}
