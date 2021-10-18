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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration

import io.circe.{Decoder, HCursor}
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.tinytypes.constraints.NonNegativeInt
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}

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
