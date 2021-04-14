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

package io.renku.eventlog.subscriptions.commitsync

import ch.datascience.graph.model.events.{CompoundEventId, LastSyncedDate}
import ch.datascience.graph.model.projects
import io.renku.eventlog.subscriptions.EventEncoder

private sealed trait CommitSyncEvent

private final case class FullCommitSyncEvent(id:             CompoundEventId,
                                             projectPath:    projects.Path,
                                             lastSyncedDate: LastSyncedDate
) extends CommitSyncEvent {
  override lazy val toString: String = s"CommitSyncEvent $id, projectPath = $projectPath, lastSynced = $lastSyncedDate"
}

private final case class MinimalCommitSyncEvent(projectId: projects.Id, projectPath: projects.Path)
    extends CommitSyncEvent {
  override lazy val toString: String = s"CommitSyncEvent projectId = $projectId, projectPath = $projectPath"
}

private object CommitSyncEventEncoder extends EventEncoder[CommitSyncEvent] {

  import io.circe.Json
  import io.circe.literal._

  override def encodeEvent(event: CommitSyncEvent): Json = event match {
    case FullCommitSyncEvent(eventId, projectPath, lastSyncedDate) => json"""{
        "categoryName": ${categoryName.value},
        "id":           ${eventId.id.value},
        "project": {
          "id":         ${eventId.projectId.value},
          "path":       ${projectPath.value}
        },
        "lastSynced":   ${lastSyncedDate.value}
      }"""
    case MinimalCommitSyncEvent(projectId, projectPath)            => json"""{
        "categoryName": ${categoryName.value},
        "project": {
          "id":         ${projectId.value},
          "path":       ${projectPath.value}
        }
      }"""
  }

  override def encodePayload(event: CommitSyncEvent): Option[String] = None
}
