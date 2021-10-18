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

import cats.Show
import cats.implicits.{showInterpolator, toShow}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{CompoundEventId, LastSyncedDate}
import io.renku.graph.model.projects

private sealed trait CommitSyncEvent

private object CommitSyncEvent {
  implicit lazy val show: Show[CommitSyncEvent] = Show.show {
    case event: FullCommitSyncEvent    => event.show
    case event: MinimalCommitSyncEvent => event.show
  }
}

private final case class FullCommitSyncEvent(id:             CompoundEventId,
                                             projectPath:    projects.Path,
                                             lastSyncedDate: LastSyncedDate
) extends CommitSyncEvent

private object FullCommitSyncEvent {
  implicit lazy val show: Show[FullCommitSyncEvent] =
    Show.show(event =>
      show"CommitSyncEvent ${event.id}, projectPath = ${event.projectPath}, lastSynced = ${event.lastSyncedDate}"
    )
}

private final case class MinimalCommitSyncEvent(project: Project) extends CommitSyncEvent

private object MinimalCommitSyncEvent {
  implicit lazy val show: Show[MinimalCommitSyncEvent] = Show.show(event => show"CommitSyncEvent ${event.project}")
}

private object CommitSyncEventEncoder {

  import io.circe.Json
  import io.circe.literal._

  def encodeEvent(event: CommitSyncEvent): Json = event match {
    case FullCommitSyncEvent(eventId, projectPath, lastSyncedDate) => json"""{
        "categoryName": ${categoryName.value},
        "id":           ${eventId.id.value},
        "project": {
          "id":         ${eventId.projectId.value},
          "path":       ${projectPath.value}
        },
        "lastSynced":   ${lastSyncedDate.value}
      }"""
    case MinimalCommitSyncEvent(Project(projectId, projectPath))   => json"""{
        "categoryName": ${categoryName.value},
        "project": {
          "id":         ${projectId.value},
          "path":       ${projectPath.value}
        }
      }"""
  }

}
