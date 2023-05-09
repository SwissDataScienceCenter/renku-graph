/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.hookdeletion.HookRemover.DeletionResult
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.HookIdentifier
import org.typelevel.log4cats.Logger

private trait HookRemover[F[_]] {
  def deleteHook(hookIdentifier: HookIdentifier, accessToken: AccessToken): F[Option[DeletionResult]]
}

private class HookRemoverImpl[F[_]: Async: Logger](projectHookFetcher: ProjectHookFetcher[F],
                                                   projectHookRemover: GLHookRemover[F]
) extends HookRemover[F] {
  import HookRemover.DeletionResult
  import projectHookRemover._
  import projectHookFetcher._

  override def deleteHook(projectHookIdentifier: HookIdentifier,
                          accessToken:           AccessToken
  ): F[Option[DeletionResult]] = {
    fetchProjectHooks(projectHookIdentifier.projectId, accessToken) >>= {
      case None => Option.empty[DeletionResult].pure[F]
      case Some(hooks) =>
        findProjectHook(projectHookIdentifier, hooks) match {
          case Some(hookToDelete) => delete(projectHookIdentifier.projectId, hookToDelete, accessToken).map(_.some)
          case None               => DeletionResult.HookNotFound.widen.some.pure[F]
        }
    }
  } onError logError(projectHookIdentifier.projectId)

  private def findProjectHook(projectHook: HookIdentifier, gitlabHooks: List[HookIdAndUrl]): Option[HookIdAndUrl] =
    gitlabHooks.find(_.url == projectHook.projectHookUrl)

  private def logError(projectId: GitLabId): PartialFunction[Throwable, F[Unit]] = { exception =>
    Logger[F].error(exception)(s"Hook deletion failed for project with id $projectId")
  }
}

private object HookRemover {

  def apply[F[_]: Async: GitLabClient: Logger]: F[HookRemover[F]] = for {
    hookRemover        <- GLHookRemover[F]
    projectHookFetcher <- ProjectHookFetcher[F]
  } yield new HookRemoverImpl[F](projectHookFetcher, hookRemover)

  sealed trait DeletionResult extends Product {
    lazy val widen: DeletionResult = this
  }

  object DeletionResult {
    final case object HookDeleted  extends DeletionResult
    final case object HookNotFound extends DeletionResult
  }
}
