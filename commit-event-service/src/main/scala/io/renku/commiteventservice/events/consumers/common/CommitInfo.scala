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

package io.renku.commiteventservice.events.consumers.common

import io.renku.graph.model.events._
import io.renku.graph.model.persons.{Email, Name}

private[consumers] case class CommitInfo(
    id:            CommitId,
    message:       CommitMessage,
    committedDate: CommittedDate,
    author:        Author,
    committer:     Committer,
    parents:       List[CommitId]
)

private[consumers] object CommitInfo {

  import cats.syntax.all._
  import io.circe._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  implicit val commitInfoDecoder: Decoder[CommitInfo] = cursor =>
    for {
      id             <- cursor.downField("id").as[CommitId]
      message        <- cursor.downField("message").as[CommitMessage]
      committedDate  <- cursor.downField("committed_date").as[CommittedDate]
      parents        <- cursor.downField("parent_ids").as[List[CommitId]]
      authorName     <- cursor.downField("author_name").as[Option[Name]]
      authorEmail    <- cursor.downField("author_email").as[Option[Email]].leftFlatMap(_ => Right(None))
      committerName  <- cursor.downField("committer_name").as[Option[Name]]
      committerEmail <- cursor.downField("committer_email").as[Option[Email]].leftFlatMap(_ => Right(None))
      author <- (authorName, authorEmail) match {
                  case (Some(name), Some(email)) => Right(Author(name, email))
                  case (Some(name), None)        => Right(Author.withName(name))
                  case (None, Some(email))       => Right(Author.withEmail(email))
                  case _                         => Left(DecodingFailure("Neither author name nor email", Nil))
                }
      committer <- (committerName, committerEmail) match {
                     case (Some(name), Some(email)) => Right(Committer(name, email))
                     case (Some(name), None)        => Right(Committer.withName(name))
                     case (None, Some(email))       => Right(Committer.withEmail(email))
                     case _                         => Left(DecodingFailure("Neither committer name nor email", Nil))
                   }
    } yield CommitInfo(id, message, committedDate, author, committer, parents)
}
