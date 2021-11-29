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

package io.renku.webhookservice.hookvalidation

import cats.effect.Async
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.{HookIdentifier, ProjectHookUrl}
import org.typelevel.log4cats.Logger

private trait ProjectHookVerifier[F[_]] {
  def checkHookPresence(
      projectHookId: HookIdentifier,
      accessToken:   AccessToken
  ): F[Boolean]
}

private object ProjectHookVerifier {

  def apply[F[_]: Async: Logger](gitlabThrottler: Throttler[F, GitLab]) = for {
    projectHookFetcher <- ProjectHookFetcher(gitlabThrottler)
  } yield new ProjectHookVerifierImpl[F](projectHookFetcher, gitlabThrottler)
}

private class ProjectHookVerifierImpl[F[_]: Async: Logger](
    projectHookFetcher: ProjectHookFetcher[F],
    gitLabThrottler:    Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with ProjectHookVerifier[F] {

  override def checkHookPresence(projectHookId: HookIdentifier, accessToken: AccessToken): F[Boolean] =
    projectHookFetcher.fetchProjectHooks(projectHookId.projectId, accessToken) map checkProjectHookExists(
      projectHookId.projectHookUrl
    )

  private def checkProjectHookExists(urlToFind: ProjectHookUrl): List[HookIdAndUrl] => Boolean = hooksIdsAndUrls =>
    hooksIdsAndUrls.map(_.url.value) contains urlToFind.value
}
