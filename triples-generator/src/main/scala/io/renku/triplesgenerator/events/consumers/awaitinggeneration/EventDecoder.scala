/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration

import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Error, ParsingFailure}
import io.circe.parser._
import io.renku.events.EventRequestContent
import io.renku.graph.model.events._
import io.renku.tinytypes.json.TinyTypeDecoders._

private trait EventDecoder {
  val decode: EventRequestContent => Either[Exception, CommitEvent]
}

private object EventDecoder extends EventDecoder {

  import io.renku.events.consumers.EventDecodingTools._

  override lazy val decode: EventRequestContent => Either[Exception, CommitEvent] = {
    case EventRequestContent.WithPayload(_, payload: String) =>
      parse(payload)
        .flatMap(_.as[CommitEvent])
        .leftMap(toMeaningfulError(payload))
    case _ =>
      new Exception("Event without or invalid payload").asLeft
  }

  private implicit val commitsDecoder: Decoder[CommitEvent] = cursor =>
    for {
      commitId <- cursor.downField("id").as[CommitId]
      project  <- cursor.value.getProject
    } yield CommitEvent(EventId(commitId.value), project, commitId)

  private def toMeaningfulError(payload: String): Error => Error = {
    case failure: DecodingFailure => failure.withMessage(s"CommitEvent cannot be decoded: '$payload'")
    case failure: ParsingFailure  => ParsingFailure(s"CommitEvent cannot be decoded: '$payload'", failure)
  }
}
