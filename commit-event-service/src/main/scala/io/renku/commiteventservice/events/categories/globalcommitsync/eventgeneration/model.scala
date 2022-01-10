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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import io.renku.commiteventservice.events.categories.globalcommitsync.CommitsCount
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.Page

private[globalcommitsync] final case class CommitWithParents(id:        CommitId,
                                                             projectId: projects.Id,
                                                             parents:   List[CommitId]
)

private[globalcommitsync] final case class ProjectCommitStats(maybeLatestCommit: Option[CommitId],
                                                              commitsCount:      CommitsCount
)

private[globalcommitsync] final case class PageResult(commits: List[CommitId], maybeNextPage: Option[Page])
private[globalcommitsync] object PageResult {
  val empty: PageResult = PageResult(commits = Nil, maybeNextPage = None)
}
