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

package io.renku.commiteventservice.events.categories

import io.renku.events.CategoryName

package object commitsync {
  val categoryName: CategoryName = CategoryName("COMMIT_SYNC")

  private[commitsync] val logMessageCommon: CommitSyncEvent => String = {
    case FullCommitSyncEvent(id, project, lastSynced) =>
      s"$categoryName: id = $id, projectId = ${project.id}, projectPath = ${project.path}, lastSynced = $lastSynced"
    case MinimalCommitSyncEvent(project) =>
      s"$categoryName: projectId = ${project.id}, projectPath = ${project.path}"
  }
}
