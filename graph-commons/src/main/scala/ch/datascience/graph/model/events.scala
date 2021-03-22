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

package ch.datascience.graph.model

import cats.syntax.all._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import ch.datascience.tinytypes.json.TinyTypeDecoders.durationDecoder
import ch.datascience.tinytypes.json.TinyTypeEncoders.durationEncoder
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.{Decoder, Encoder}
import io.circe.Decoder.decodeString

import java.time.{Clock, Duration, Instant}

object events {

  final class CategoryName private (val value: String) extends AnyVal with StringTinyType
  implicit object CategoryName extends TinyTypeFactory[CategoryName](new CategoryName(_)) with NonBlank

  final case class CompoundEventId(id: EventId, projectId: projects.Id) {
    override lazy val toString: String = s"id = $id, projectId = $projectId"
  }

  final class CommitId private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitId extends TinyTypeFactory[CommitId](new CommitId(_)) with GitSha

  final class CommitMessage private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitMessage extends TinyTypeFactory[CommitMessage](new CommitMessage(_)) with NonBlank

  final class CommittedDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object CommittedDate extends TinyTypeFactory[CommittedDate](new CommittedDate(_)) with BoundedInstant {
    import java.time.temporal.ChronoUnit.HOURS
    protected[this] override def maybeMax: Option[Instant] = now.plus(24, HOURS).some
  }

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

  sealed trait EventStatus extends StringTinyType with Product with Serializable
  object EventStatus extends TinyTypeFactory[EventStatus](EventStatusInstantiator) {

    val all: Set[EventStatus] =
      Set(
        New,
        GeneratingTriples,
        TriplesGenerated,
        TransformingTriples,
        TriplesStore,
        Skipped,
        GenerationRecoverableFailure,
        GenerationNonRecoverableFailure,
        TransformationRecoverableFailure,
        TransformationNonRecoverableFailure
      )

    type New = New.type
    final case object New extends EventStatus {
      override val value: String = "NEW"
    }

    type GeneratingTriples = GeneratingTriples.type
    final case object GeneratingTriples extends EventStatus {
      override val value: String = "GENERATING_TRIPLES"
    }

    type TriplesGenerated = TriplesGenerated.type
    final case object TriplesGenerated extends EventStatus {
      override val value: String = "TRIPLES_GENERATED"
    }

    type TransformingTriples = TransformingTriples.type
    final case object TransformingTriples extends EventStatus {
      override val value: String = "TRANSFORMING_TRIPLES"
    }

    sealed trait FinalStatus extends EventStatus

    final case object TriplesStore extends EventStatus with FinalStatus {
      override val value: String = "TRIPLES_STORE"
    }
    final case object Skipped extends EventStatus with FinalStatus {
      override val value: String = "SKIPPED"
    }

    sealed trait FailureStatus extends EventStatus

    final case object GenerationRecoverableFailure extends FailureStatus {
      override val value: String = "GENERATION_RECOVERABLE_FAILURE"
    }
    type GenerationRecoverableFailure = GenerationRecoverableFailure.type

    final case object GenerationNonRecoverableFailure extends FailureStatus with FinalStatus {
      override val value: String = "GENERATION_NON_RECOVERABLE_FAILURE"
    }
    type GenerationNonRecoverableFailure = GenerationNonRecoverableFailure.type

    final case object TransformationRecoverableFailure extends FailureStatus {
      override val value: String = "TRANSFORMATION_RECOVERABLE_FAILURE"
    }
    type TransformationRecoverableFailure = TransformationRecoverableFailure.type

    final case object TransformationNonRecoverableFailure extends FailureStatus with FinalStatus {
      override val value: String = "TRANSFORMATION_NON_RECOVERABLE_FAILURE"
    }
    type TransformationNonRecoverableFailure = TransformationNonRecoverableFailure.type

    implicit val eventStatusDecoder: Decoder[EventStatus] = decodeString.emap { value =>
      Either.fromOption(
        EventStatus.all.find(_.value == value),
        ifNone = s"'$value' unknown EventStatus"
      )
    }
  }

  private object EventStatusInstantiator extends (String => EventStatus) {
    override def apply(value: String): EventStatus = EventStatus.all.find(_.value == value).getOrElse {
      throw new IllegalArgumentException(s"'$value' unknown EventStatus")
    }
  }

  final class EventProcessingTime private (val value: Duration) extends AnyVal with DurationTinyType
  object EventProcessingTime
      extends TinyTypeFactory[EventProcessingTime](new EventProcessingTime(_))
      with DurationNotNegative {

    implicit val decoder: Decoder[EventProcessingTime] = durationDecoder(EventProcessingTime)
    implicit val encoder: Encoder[EventProcessingTime] = durationEncoder

    implicit class EventProcessingTimeOps(processingTime: EventProcessingTime) {

      def *(multiplier: Int Refined Positive): EventProcessingTime =
        EventProcessingTime(Duration.ofMillis(processingTime.value.toMillis * multiplier.value))

      def /(multiplier: Int Refined Positive): EventProcessingTime =
        EventProcessingTime(Duration.ofMillis(processingTime.value.toMillis / multiplier.value))
    }
  }
}
