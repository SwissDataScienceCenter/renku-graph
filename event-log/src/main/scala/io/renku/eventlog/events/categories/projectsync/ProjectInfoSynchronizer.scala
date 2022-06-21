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

package io.renku.eventlog.events.categories
package projectsync

import cats.effect.Async
import cats.syntax.all._
import io.renku.events.EventRequestContent
import io.renku.events.producers.EventSender
import io.renku.graph.model.projects
import org.typelevel.log4cats.Logger

private trait ProjectInfoSynchronizer[F[_]] {
  def syncProjectInfo(event: ProjectSyncEvent): F[Unit]
}

private class ProjectInfoSynchronizerImpl[F[_]: Async: Logger](
    gitLabProjectFetcher: GitLabProjectFetcher[F],
    dbUpdater:            DBUpdater[F],
    eventSender:          EventSender[F]
) extends ProjectInfoSynchronizer[F] {

  import eventSender._
  import gitLabProjectFetcher._
  import io.circe.literal._

  override def syncProjectInfo(event: ProjectSyncEvent): F[Unit] = fetchGitLabProject(event.projectId) >>= {
    case Right(Some(event.projectPath)) => ().pure[F]
    case Right(Some(newPath)) =>
      dbUpdater.update(event.projectId, newPath) >>
        send(globalCommitSyncRequest(event.projectId, newPath)) >>
        send(cleanUpRequest(event.projectPath))
    case Right(None)     => send(cleanUpRequest(event.projectPath))
    case Left(exception) => Logger[F].info(show"$categoryName: $event failed: $exception")
  }

  private def send: ((EventRequestContent.NoPayload, EventSender.EventContext)) => F[Unit] = {
    case (payload, eventCtx) => sendEvent(payload, eventCtx)
  }

  private def globalCommitSyncRequest(projectId: projects.Id, newPath: projects.Path) = {
    val category = globalcommitsyncrequest.categoryName
    val payload = EventRequestContent.NoPayload(json"""{
      "categoryName": ${category.show},
      "project": {
        "id":   ${projectId.value},
        "path": ${newPath.value}
      }
    }""")
    val context = EventSender.EventContext(category, errorMessage = show"$categoryName: sending $category failed")
    payload -> context
  }

  private def cleanUpRequest(oldPath: projects.Path) = {
    val category = cleanuprequest.categoryName
    val payload = EventRequestContent.NoPayload(json"""{
      "categoryName": ${category.show},
      "project": {
        "path": ${oldPath.value}
      }
    }""")
    val context = EventSender.EventContext(category, errorMessage = show"$categoryName: sending $category failed")
    payload -> context
  }
}
