/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model

import java.time.{Clock, Instant}

import cats.syntax.all._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

object events {

  final case class CompoundEventId(id: EventId, projectId: projects.Id) {
    override lazy val toString: String = s"id = $id, projectId = $projectId"
  }

  final class CommitId private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitId extends TinyTypeFactory[CommitId](new CommitId(_)) with GitSha

  final class CommitMessage private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitMessage extends TinyTypeFactory[CommitMessage](new CommitMessage(_)) with NonBlank

  final class CommittedDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object CommittedDate extends TinyTypeFactory[CommittedDate](new CommittedDate(_)) with InstantNotInTheFuture

  final class EventId private (val value: String) extends AnyVal with StringTinyType
  implicit object EventId extends TinyTypeFactory[EventId](new EventId(_)) with NonBlank

  final class EventBody private (val value: String) extends AnyVal with StringTinyType
  implicit object EventBody extends TinyTypeFactory[EventBody](new EventBody(_)) with NonBlank {

    implicit class EventBodyOps(eventBody: EventBody) {

      import io.circe.parser.parse
      import io.circe.{Decoder, DecodingFailure, Json}

      def decodeAs[O](implicit decoder: Decoder[O]): Either[DecodingFailure, O] =
        bodyAsJson flatMap (_.as[O])

      private val bodyAsJson: Either[DecodingFailure, Json] =
        parse(eventBody.value).leftMap(_ => DecodingFailure("Cannot parse event body", Nil))
    }
  }

  final class BatchDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object BatchDate extends TinyTypeFactory[BatchDate](new BatchDate(_)) with InstantNotInTheFuture {
    def apply(clock: Clock): BatchDate = apply(clock.instant())
  }
}
