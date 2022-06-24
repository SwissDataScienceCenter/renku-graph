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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration

import cats.MonadThrow
import cats.syntax.all._
import io.circe.parser._
import io.circe.{Decoder, DecodingFailure, Error, ParsingFailure}
import io.renku.events.consumers.Project
import io.renku.graph.model.events._
import io.renku.graph.model.projects.{Id, Path}
import io.renku.tinytypes.json.TinyTypeDecoders._

private trait EventBodyDeserializer[F[_]] {
  def toCommitEvent(eventBody: EventBody): F[CommitEvent]
}

private class EventBodyDeserializerImpl[F[_]: MonadThrow] extends EventBodyDeserializer[F] {

  override def toCommitEvent(eventBody: EventBody): F[CommitEvent] =
    MonadThrow[F].fromEither {
      parse(eventBody.value)
        .flatMap(_.as[CommitEvent])
        .leftMap(toMeaningfulError(eventBody))
    }

  private implicit val commitsDecoder: Decoder[CommitEvent] = cursor =>
    for {
      commitId    <- cursor.downField("id").as[CommitId]
      projectId   <- cursor.downField("project").downField("id").as[Id]
      projectPath <- cursor.downField("project").downField("path").as[Path]
    } yield CommitEvent(EventId(commitId.value), Project(projectId, projectPath), commitId)

  private def toMeaningfulError(eventBody: EventBody): Error => Error = {
    case failure: DecodingFailure => failure.withMessage(s"CommitEvent cannot be deserialised: '$eventBody'")
    case failure: ParsingFailure  => ParsingFailure(s"CommitEvent cannot be deserialised: '$eventBody'", failure)
  }
}

private object EventBodyDeserializer {
  def apply[F[_]: MonadThrow]: EventBodyDeserializer[F] = new EventBodyDeserializerImpl[F]
}
