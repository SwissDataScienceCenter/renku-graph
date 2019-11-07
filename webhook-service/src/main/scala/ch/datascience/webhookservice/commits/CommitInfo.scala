/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.commits

import ch.datascience.graph.model.events._
import ch.datascience.graph.model.users.{Email, Username}

case class CommitInfo(
    id:            CommitId,
    message:       CommitMessage,
    committedDate: CommittedDate,
    author:        User,
    committer:     User,
    parents:       List[CommitId]
)

object CommitInfo {

  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._

  private[commits] implicit val commitInfoDecoder: Decoder[CommitInfo] = (cursor: HCursor) =>
    for {
      id             <- cursor.downField("id").as[CommitId]
      authorName     <- cursor.downField("author_name").as[Username]
      authorEmail    <- cursor.downField("author_email").as[Email]
      committerName  <- cursor.downField("committer_name").as[Username]
      committerEmail <- cursor.downField("committer_email").as[Email]
      message        <- cursor.downField("message").as[CommitMessage]
      committedDate  <- cursor.downField("committed_date").as[CommittedDate]
      parents        <- cursor.downField("parent_ids").as[List[CommitId]]
    } yield CommitInfo(id,
                       message,
                       committedDate,
                       author    = User(authorName, authorEmail),
                       committer = User(committerName, committerEmail),
                       parents)
}
