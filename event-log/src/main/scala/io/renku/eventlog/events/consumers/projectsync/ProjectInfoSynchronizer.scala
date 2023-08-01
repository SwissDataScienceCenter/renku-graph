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

package io.renku.eventlog.events.consumers
package projectsync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.EventRequestContent
import io.renku.events.producers.EventSender
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.typelevel.log4cats.Logger

private trait ProjectInfoSynchronizer[F[_]] {
  def syncProjectInfo(event: ProjectSyncEvent): F[Unit]
}

private class ProjectInfoSynchronizerImpl[F[_]: MonadThrow: Logger](
    gitLabProjectFetcher: GitLabProjectFetcher[F],
    projectRemover:       ProjectRemover[F],
    eventSender:          EventSender[F],
    tgClient:             triplesgenerator.api.events.Client[F]
) extends ProjectInfoSynchronizer[F] {

  import eventSender._
  import gitLabProjectFetcher._
  import io.circe.literal._
  import projectRemover._

  override def syncProjectInfo(event: ProjectSyncEvent): F[Unit] = fetchGitLabProject(event.projectId) >>= {
    case Right(Some(event.`projectSlug`)) => tgClient.send(SyncRepoMetadata(event.projectSlug))
    case Right(Some(newPath)) =>
      removeProject(event.projectId) >>
        send(cleanUpRequest(event)) >>
        send(commitSyncRequest(event.projectId, newPath))
    case Right(None)     => send(cleanUpRequest(event))
    case Left(exception) => Logger[F].info(show"$categoryName: $event failed: $exception")
  }

  private def send: ((EventRequestContent.NoPayload, EventSender.EventContext)) => F[Unit] = {
    case (payload, eventCtx) => sendEvent(payload, eventCtx)
  }

  private def commitSyncRequest(projectId: projects.GitLabId, newSlug: projects.Slug) = {
    val category = commitsyncrequest.categoryName
    val payload = EventRequestContent.NoPayload(json"""{
      "categoryName": $category,
      "project": {
        "id":   $projectId,
        "slug": $newSlug
      }
    }""")
    val context = EventSender.EventContext(category, errorMessage = show"$categoryName: sending $category failed")
    payload -> context
  }

  private def cleanUpRequest(event: ProjectSyncEvent) = {
    val category = cleanuprequest.categoryName
    val payload = EventRequestContent.NoPayload(json"""{
      "categoryName": $category,
      "project": {
        "id":   ${event.projectId},
        "slug": ${event.projectSlug}
      }
    }""")
    val context = EventSender.EventContext(category, errorMessage = show"$categoryName: sending $category failed")
    payload -> context
  }
}

private object ProjectInfoSynchronizer {
  def apply[F[
      _
  ]: Async: GitLabClient: AccessTokenFinder: SessionResource: Logger: MetricsRegistry: QueriesExecutionTimes]
      : F[ProjectInfoSynchronizer[F]] = for {
    gitLabProjectFetcher <- GitLabProjectFetcher[F]
    projectRemover       <- ProjectRemover[F]
    eventSender          <- EventSender[F](EventLogUrl)
    tgClient             <- triplesgenerator.api.events.Client[F]
  } yield new ProjectInfoSynchronizerImpl(gitLabProjectFetcher, projectRemover, eventSender, tgClient)
}
