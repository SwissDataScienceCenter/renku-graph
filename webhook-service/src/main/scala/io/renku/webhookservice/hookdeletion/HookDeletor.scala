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

package io.renku.webhookservice.hookdeletion

import cats.effect._
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects.Id
import io.renku.http.client.AccessToken
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait HookDeletor[F[_]] {
  def deleteHook(projectId: Id, accessToken: AccessToken): F[DeletionResult]
}

private class HookDeletorImpl[F[_]: Spawn: Logger](projectHookDeletor: ProjectHookDeletor[F]) extends HookDeletor[F] {
  import HookDeletor.DeletionResult
  import projectHookDeletor._

  override def deleteHook(projectId: Id, accessToken: AccessToken): F[DeletionResult] =
    delete(projectId, accessToken) recoverWith loggingError(projectId)

  private def loggingError(projectId: Id): PartialFunction[Throwable, F[DeletionResult]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Hook deletion failed for project with id $projectId") >>
      exception.raiseError[F, DeletionResult]
  }

}

private object HookDeletor {

  def apply[F[_]: Async: Logger](
      gitLabThrottler: Throttler[F, GitLab]
  ): F[HookDeletor[F]] = for {
    hookDeletor <- ProjectHookDeletor[F](gitLabThrottler)
  } yield new HookDeletorImpl[F](hookDeletor)

  sealed trait DeletionResult extends Product with Serializable

  object DeletionResult {
    final case object HookDeleted  extends DeletionResult
    final case object HookNotFound extends DeletionResult
  }
}
