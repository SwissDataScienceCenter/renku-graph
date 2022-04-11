/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.HookIdentifier
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait HookDeletor[F[_]] {
  def deleteHook(hookIdentifier: HookIdentifier, accessToken: AccessToken): F[DeletionResult]
}

private class HookDeletorImpl[F[_]: Spawn: Logger](projectHookFetcher: ProjectHookFetcher[F],
                                                   projectHookDeletor: ProjectHookDeletor[F]
) extends HookDeletor[F] {
  import HookDeletor.DeletionResult
  import projectHookDeletor._

  override def deleteHook(projectHookIdentifier: HookIdentifier, accessToken: AccessToken): F[DeletionResult] = (for {
    gitlabProjectHooks <- projectHookFetcher.fetchProjectHooks(projectHookIdentifier.projectId, accessToken)
    result <- findProjectHook(projectHookIdentifier, gitlabProjectHooks) match {
                case Some(hookToDelete) => delete(projectHookIdentifier.projectId, hookToDelete, accessToken)
                case None               => DeletionResult.HookNotFound.pure[F].widen[DeletionResult]
              }
  } yield result) recoverWith loggingError(projectHookIdentifier.projectId)

  private def findProjectHook(projectHook: HookIdentifier, gitlabHooks: List[HookIdAndUrl]): Option[HookIdAndUrl] =
    gitlabHooks.find(_.url == projectHook.projectHookUrl)

  private def loggingError(projectId: Id): PartialFunction[Throwable, F[DeletionResult]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Hook deletion failed for project with id $projectId") >>
      exception.raiseError[F, DeletionResult]
  }

}

private object HookDeletor {

  def apply[F[_]: Async: Logger](
      gitLabClient: GitLabClient[F]
  ): F[HookDeletor[F]] = for {
    hookDeletor        <- ProjectHookDeletor[F](gitLabClient)
    projectHookFetcher <- ProjectHookFetcher(gitLabClient)
  } yield new HookDeletorImpl[F](projectHookFetcher, hookDeletor)

  sealed trait DeletionResult extends Product with Serializable

  object DeletionResult {
    final case object HookDeleted  extends DeletionResult
    final case object HookNotFound extends DeletionResult
  }
}
