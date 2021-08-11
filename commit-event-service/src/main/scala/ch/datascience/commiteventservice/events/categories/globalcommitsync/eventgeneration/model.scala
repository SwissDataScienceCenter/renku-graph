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

package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.constraints.NonNegativeInt
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import io.circe.{Decoder, HCursor}

private[globalcommitsync] final case class CommitWithParents(id:        CommitId,
                                                             projectId: projects.Id,
                                                             parents:   List[CommitId]
)

private[globalcommitsync] final case class ProjectCommitStats(maybeLatestCommit: Option[CommitId],
                                                              commitCount:       CommitCount
)

private[globalcommitsync] object ProjectCommitStats {
  implicit val commitCountDecoder: Decoder[CommitCount] = (cursor: HCursor) =>
    cursor.downField("statistics").downField("commit_count").as[CommitCount]

  private[globalcommitsync] final class CommitCount private (val value: Int) extends AnyVal with IntTinyType
  private[globalcommitsync] implicit object CommitCount
      extends TinyTypeFactory[CommitCount](new CommitCount(_))
      with NonNegativeInt
}
