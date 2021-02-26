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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.data.NonEmptyList
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.users.{Email, Name}

private final case class CommitPersonsInfo(
    id:         CommitId,
    committers: NonEmptyList[CommitPerson]
)

private object CommitPersonsInfo {

  import cats.syntax.all._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._

  private[persondetails] implicit val commitInfoDecoder: Decoder[CommitPersonsInfo] = (cursor: HCursor) => {

    implicit class CursorOps(cursor: ACursor) {
      lazy val toMaybeName: Decoder.Result[Option[Name]] =
        cursor.as[Option[String]].map(blankToNone).flatMap(toOption[Name])
      lazy val toMaybeEmail: Decoder.Result[Option[Email]] =
        cursor.as[Option[String]].map(blankToNone).flatMap(toOption[Email]).leftFlatMap(_ => Right(None))
    }

    for {
      id             <- cursor.downField("id").as[CommitId]
      authorName     <- cursor.downField("author_name").toMaybeName
      authorEmail    <- cursor.downField("author_email").toMaybeEmail
      committerName  <- cursor.downField("committer_name").toMaybeName
      committerEmail <- cursor.downField("committer_email").toMaybeEmail
      author <- (authorName, authorEmail) match {
                  case (Some(name), Some(email)) => Right(CommitPerson(name, email).some)
                  case _                         => Right(None)
                }
      committer <- (committerName, committerEmail) match {
                     case (Some(name), Some(email)) => Right(CommitPerson(name, email).some)
                     case _                         => Right(None)
                   }
      commitInfo <- (author, committer) match {
                      case (Some(author), Some(committer)) =>
                        Right(CommitPersonsInfo(id, NonEmptyList(author, committer +: Nil)))
                      case (Some(author), None)    => Right(CommitPersonsInfo(id, NonEmptyList(author, Nil)))
                      case (None, Some(committer)) => Right(CommitPersonsInfo(id, NonEmptyList(committer, Nil)))
                      case _                       => Left(DecodingFailure(s"No valid author and committer on the commit $id", Nil))
                    }

    } yield commitInfo
  }
}
