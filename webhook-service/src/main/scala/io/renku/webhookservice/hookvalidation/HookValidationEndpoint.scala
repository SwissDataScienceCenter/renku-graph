/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.hookvalidation

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.graph.model.projects.GitLabId
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.hookvalidation
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.{HookExists, HookMissing}
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait HookValidationEndpoint[F[_]] {
  def validateHook(projectId: GitLabId, authUser: AuthUser): F[Response[F]]
}

class HookValidationEndpointImpl[F[_]: MonadThrow: Logger](hookValidator: HookValidator[F])
    extends Http4sDsl[F]
    with HookValidationEndpoint[F]
    with RenkuEntityCodec {

  def validateHook(projectId: GitLabId, authUser: AuthUser): F[Response[F]] = {
    hookValidator.validateHook(projectId, Some(authUser.accessToken)).flatMap(toHttpResponse(_))
  } handleErrorWith withHttpResult

  private lazy val toHttpResponse: Option[HookValidationResult] => F[Response[F]] = {
    case Some(HookExists)  => Ok(Message.Info("Hook valid"))
    case Some(HookMissing) => NotFound(Message.Info("Hook not found"))
    case None              => Response[F](Unauthorized).withEntity(Message.Error("Unauthorized")).pure[F]
  }

  private lazy val withHttpResult: Throwable => F[Response[F]] = { exception =>
    Logger[F].error(exception)(exception.getMessage) >>
      InternalServerError(Message.Error.fromExceptionMessage(exception))
  }
}

object HookValidationEndpoint {
  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry](
      projectHookUrl: ProjectHookUrl
  ): F[HookValidationEndpoint[F]] = for {
    hookValidator <- hookvalidation.HookValidator(projectHookUrl)
  } yield new HookValidationEndpointImpl[F](hookValidator)
}
