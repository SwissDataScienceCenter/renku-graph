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

private final case class CommitSyncEvent(id:             CompoundEventId,
                                         projectPath:    projects.Path,
                                         lastSyncedDate: LastSyncedDate
) {
  override lazy val toString: String = s"$CommitSyncEvent $id, projectPath = $projectPath, lastSynced = $lastSyncedDate"
}

private object CommitSyncEventEncoder extends EventEncoder[CommitSyncEvent] {

  import io.circe.Json
  import io.circe.literal._

  override def encodeEvent(event: CommitSyncEvent): Json = json"""{
    "categoryName": ${categoryName.value},
    "id":           ${event.id.id.value},
    "project": {
      "id":         ${event.id.projectId.value},
      "path":       ${event.projectPath.value}
    },
    "lastSynced":   ${event.lastSyncedDate.value}
  }"""

  override def encodePayload(event: CommitSyncEvent): Option[String] = None
}
