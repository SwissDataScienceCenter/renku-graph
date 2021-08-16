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

package io.renku.eventlog.subscriptions.globalcommitsync

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.CommitId
import io.renku.eventlog.subscriptions.EventEncoder

private final case class GlobalCommitSyncEvent(project: Project, commits: List[CommitId]) {
  override lazy val toString: String =
    s"GlobalCommitSyncEvent projectId = ${project.id}, projectPath = ${project.path}, numberOfCommits = ${commits.length}"
}

private object GlobalCommitSyncEventEncoder extends EventEncoder[GlobalCommitSyncEvent] {

  import io.circe.Json
  import io.circe.literal._

  override def encodeEvent(event: GlobalCommitSyncEvent): Json = json"""{
        "categoryName": ${categoryName.value},
        "project": {
          "id":         ${event.project.id.value},
          "path":       ${event.project.path.value}
        },
        "commits":      ${event.commits.map(_.value)}
      }"""

  override def encodePayload(event: GlobalCommitSyncEvent): Option[String] = None
}
