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

package io.renku.eventlog.events.consumers
package projectsync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.{CleanUpRequest, GlobalCommitSyncRequest}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import io.renku.{eventlog, triplesgenerator}
import org.typelevel.log4cats.Logger

private trait ProjectInfoSynchronizer[F[_]] {
  def syncProjectInfo(event: ProjectSyncEvent): F[Unit]
}

private class ProjectInfoSynchronizerImpl[F[_]: MonadThrow: Logger](
    gitLabProjectFetcher: GitLabProjectFetcher[F],
    projectRemover:       ProjectRemover[F],
    elClient:             eventlog.api.events.Client[F],
    tgClient:             triplesgenerator.api.events.Client[F]
) extends ProjectInfoSynchronizer[F] {

  import gitLabProjectFetcher._
  import projectRemover._

  override def syncProjectInfo(event: ProjectSyncEvent): F[Unit] =
    fetchGitLabProject(event.projectId) >>= {
      case Right(Some(event.`projectSlug`)) => tgClient.send(SyncRepoMetadata(event.projectSlug))
      case Right(Some(newSlug)) =>
        removeProject(event.projectId) >>
          elClient.send(CleanUpRequest(event.projectId, event.projectSlug)) >>
          elClient.send(GlobalCommitSyncRequest(event.projectId, newSlug))
      case Right(None)     => elClient.send(GlobalCommitSyncRequest(event.projectId, event.projectSlug))
      case Left(exception) => Logger[F].info(show"$categoryName: $event failed: $exception")
    }
}

private object ProjectInfoSynchronizer {
  def apply[F[_]: Async: GitLabClient: SessionResource: Logger: MetricsRegistry: QueriesExecutionTimes]
      : F[ProjectInfoSynchronizer[F]] = for {
    gitLabProjectFetcher <- GitLabProjectFetcher[F]
    projectRemover       <- ProjectRemover[F]
    elClient             <- eventlog.api.events.Client[F]
    tgClient             <- triplesgenerator.api.events.Client[F]
  } yield new ProjectInfoSynchronizerImpl(gitLabProjectFetcher, projectRemover, elClient, tgClient)
}
