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

package ch.datascience.commiteventservice.events.categories.commitsync

import ch.datascience.graph.model.events.{CommitId, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path, Visibility}

private final case class CommitProject(id: projects.Id, path: projects.Path)

private sealed trait CommitSyncEvent { val project: CommitProject }

private final case class FullCommitSyncEvent(id:                   CommitId,
                                             override val project: CommitProject,
                                             lastSynced:           LastSyncedDate
) extends CommitSyncEvent {
  override lazy val toString: String =
    s"id = $id, projectId = ${project.id}, projectPath = ${project.path}, lastSynced = $lastSynced"
}
private final case class MinimalCommitSyncEvent(override val project: CommitProject) extends CommitSyncEvent {
  override lazy val toString: String =
    s"projectId = ${project.id}, projectPath = ${project.path}"
}

private final case class ProjectInfo(
    id:         Id,
    visibility: Visibility,
    path:       Path
)
