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

package io.renku.commiteventservice.events.categories.globalcommitsync

import GlobalCommitSyncEvent.CommitsInfo
import cats.Show
import cats.syntax.all._
import io.renku.events.consumers.Project
import io.renku.graph.model.events.CommitId
import io.renku.tinytypes.constraints.NonNegativeLong
import io.renku.tinytypes.{LongTinyType, TinyTypeFactory}

private final case class GlobalCommitSyncEvent(project: Project, commits: CommitsInfo)

private object GlobalCommitSyncEvent {
  final case class CommitsInfo(count: CommitsCount, latest: CommitId)

  implicit lazy val show: Show[GlobalCommitSyncEvent] = Show.show { event =>
    show"projectId = ${event.project.id}, projectPath = ${event.project.path}, " +
      show"numberOfCommits = ${event.commits.count}, latestCommit = ${event.commits.latest}"
  }
}

final class CommitsCount private (val value: Long) extends AnyVal with LongTinyType
object CommitsCount extends TinyTypeFactory[CommitsCount](new CommitsCount(_)) with NonNegativeLong {
  import io.circe.Decoder
  import io.renku.tinytypes.json.TinyTypeDecoders.longDecoder

  implicit val decoder: Decoder[CommitsCount] = longDecoder(CommitsCount)
}
