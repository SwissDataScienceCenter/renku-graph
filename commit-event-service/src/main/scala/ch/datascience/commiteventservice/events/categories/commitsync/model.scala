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

import ch.datascience.graph.model.events.{CategoryName, CommitId, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path, Visibility}

private final case class CommitProject(id: projects.Id, path: projects.Path)

private final case class CommitSyncEvent(categoryName: CategoryName,
                                         id:           CommitId,
                                         project:      CommitProject,
                                         lastSynced:   LastSyncedDate
)

private final case class ProjectInfo(
    id:         Id,
    visibility: Visibility,
    path:       Path
)
