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

package ch.datascience.graph.events

import java.time.Instant

import ch.datascience.tinytypes.constraints.{GitSha, InstantInThePast, NonBlank}
import ch.datascience.tinytypes.json._
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import io.circe.Decoder
import play.api.libs.json.{Format, Json, Writes}

final case class CommitEvent(
    id:            CommitId,
    message:       CommitMessage,
    committedDate: CommittedDate,
    pushUser:      PushUser,
    author:        User,
    committer:     User,
    parents:       Seq[CommitId],
    project:       Project
)

object CommitEvent {
  private implicit val userWrites:     Writes[User]        = Json.writes[User]
  private implicit val pushUserWrites: Writes[PushUser]    = Json.writes[PushUser]
  private implicit val projectWrites:  Writes[Project]     = Json.writes[Project]
  implicit val commitEventWrites:      Writes[CommitEvent] = Json.writes[CommitEvent]
}

final class CommitId private (val value: String) extends AnyVal with TinyType[String]
object CommitId extends TinyTypeFactory[String, CommitId](new CommitId(_)) with GitSha {
  implicit lazy val commitIdFormat:  Format[CommitId]  = TinyTypeFormat(CommitId.apply)
  implicit lazy val commitIdDecoder: Decoder[CommitId] = Decoder.decodeString.map(CommitId.apply)
}

final class CommitMessage private (val value: String) extends AnyVal with TinyType[String]
object CommitMessage extends TinyTypeFactory[String, CommitMessage](new CommitMessage(_)) with NonBlank {
  implicit lazy val commitMessageFormat:  Format[CommitMessage]  = TinyTypeFormat(CommitMessage.apply)
  implicit lazy val commitMessageDecoder: Decoder[CommitMessage] = Decoder.decodeString.map(CommitMessage.apply)
}

final class CommittedDate private (val value: Instant) extends AnyVal with TinyType[Instant]
object CommittedDate extends TinyTypeFactory[Instant, CommittedDate](new CommittedDate(_)) with InstantInThePast {
  implicit lazy val committedDateFormat: Format[CommittedDate] = TinyTypeFormat(CommittedDate.apply)
  implicit lazy val committedDateDecoder: Decoder[CommittedDate] =
    Decoder.decodeZonedDateTime.map(t => CommittedDate(t.toInstant))
}

final class HookAccessToken private (val value: String) extends AnyVal with TinyType[String]
object HookAccessToken extends TinyTypeFactory[String, HookAccessToken](new HookAccessToken(_)) with NonBlank {
  implicit lazy val hookAccessTokenDecoder: Decoder[HookAccessToken] = Decoder.decodeString.map(HookAccessToken.apply)
}
