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

package io.renku.graph.model

import cats.Show
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.{Decoder, Encoder}
import io.circe.Decoder.decodeString
import io.renku.data.ErrorMessage
import io.renku.graph.model.events.EventInfo.ProjectIds
import io.renku.tinytypes._
import io.renku.tinytypes.constraints._
import io.renku.tinytypes.contenttypes.ZippedContent
import io.renku.tinytypes.json.TinyTypeDecoders._

import java.time.{Clock, Duration, Instant}
import scala.math.BigDecimal.RoundingMode

object events {

  final class CreatedDate private (val value: Instant) extends AnyVal with InstantTinyType

  object CreatedDate extends TinyTypeFactory[CreatedDate](new CreatedDate(_)) with InstantNotInTheFuture[CreatedDate]

  final case class CompoundEventId(id: EventId, projectId: projects.GitLabId) {
    override lazy val toString: String = s"id = $id, projectId = $projectId"
  }

  object CompoundEventId {
    implicit lazy val show: Show[CompoundEventId] = Show.show(id => show"id = ${id.id}, projectId = ${id.projectId}")
  }

  final case class EventDetails(id: EventId, projectId: projects.GitLabId, eventBody: EventBody) {
    override lazy val toString: String          = s"id = $id, projectId = $projectId"
    lazy val compoundEventId:   CompoundEventId = CompoundEventId(id, projectId)
  }

  object EventDetails {
    def apply(compoundEventId: CompoundEventId, eventBody: EventBody): EventDetails =
      EventDetails(compoundEventId.id, compoundEventId.projectId, eventBody)
  }

  final class CommitId private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitId extends TinyTypeFactory[CommitId](new CommitId(_)) with GitSha[CommitId]

  final class CommitMessage private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitMessage
      extends TinyTypeFactory[CommitMessage](new CommitMessage(_))
      with NonBlank[CommitMessage]

  final class CommittedDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object CommittedDate
      extends TinyTypeFactory[CommittedDate](new CommittedDate(_))
      with BoundedInstant[CommittedDate] {
    import java.time.temporal.ChronoUnit.HOURS
    protected[this] override def maybeMax: Option[Instant] = instantNow.plus(24, HOURS).some
  }

  final class EventId private (val value: String) extends AnyVal with StringTinyType
  implicit object EventId extends TinyTypeFactory[EventId](new EventId(_)) with NonBlank[EventId]

  final class EventBody private (val value: String) extends AnyVal with StringTinyType
  implicit object EventBody extends TinyTypeFactory[EventBody](new EventBody(_)) with NonBlank[EventBody] {

    implicit class EventBodyOps(eventBody: EventBody) {

      import io.circe.parser.parse
      import io.circe.{Decoder, DecodingFailure, Json}

      def decodeAs[O](implicit decoder: Decoder[O]): Either[DecodingFailure, O] =
        bodyAsJson flatMap (_.as[O])

      private val bodyAsJson: Either[DecodingFailure, Json] =
        parse(eventBody.value).leftMap(_ => DecodingFailure("Cannot parse event body", Nil))
    }
  }

  final class ZippedEventPayload private (val value: Array[Byte])
      extends AnyVal
      with ByteArrayTinyType
      with ZippedContent

  implicit object ZippedEventPayload extends TinyTypeFactory[ZippedEventPayload](new ZippedEventPayload(_)) {
    val empty: ZippedEventPayload = ZippedEventPayload(new Array[Byte](0))
  }

  final class BatchDate private (val value: Instant) extends AnyVal with InstantTinyType

  implicit object BatchDate extends TinyTypeFactory[BatchDate](new BatchDate(_)) with InstantNotInTheFuture[BatchDate] {
    def apply(clock: Clock): BatchDate = apply(clock.instant())
  }

  final class EventDate private (val value: Instant) extends AnyVal with InstantTinyType
  object EventDate extends TinyTypeFactory[EventDate](new EventDate(_)) with BoundedInstant[EventDate] {
    import java.time.temporal.ChronoUnit.HOURS

    protected[this] override def maybeMax: Option[Instant] = Some(instantNow.plus(24, HOURS))

    implicit val decoder: Decoder[EventDate] = instantDecoder(EventDate)
  }

  final class EventMessage private (val value: String) extends AnyVal with StringTinyType
  object EventMessage extends TinyTypeFactory[EventMessage](new EventMessage(_)) with NonBlank[EventMessage] {

    implicit val decoder: Decoder[EventMessage] = stringDecoder(EventMessage)
    implicit val encoder: Encoder[EventMessage] = Encoder.encodeString.contramap(_.value)

    def apply(exception: Throwable): EventMessage = EventMessage(ErrorMessage.withStackTrace(exception).value)
  }

  final class ExecutionDate private (val value: Instant) extends AnyVal with InstantTinyType
  object ExecutionDate extends TinyTypeFactory[ExecutionDate](new ExecutionDate(_)) {
    implicit val decoder: Decoder[ExecutionDate] = instantDecoder(ExecutionDate)
  }

  sealed trait EventStatus extends StringTinyType with Product with Serializable

  object EventStatus extends TinyTypeFactory[EventStatus](EventStatusInstantiator) {

    val all: Set[EventStatus] = Set(
      New,
      Skipped,
      GeneratingTriples,
      GenerationRecoverableFailure,
      GenerationNonRecoverableFailure,
      TriplesGenerated,
      TransformingTriples,
      TransformationRecoverableFailure,
      TransformationNonRecoverableFailure,
      TriplesStore,
      AwaitingDeletion,
      Deleting
    )

    val statusesOrdered: Seq[EventStatus] = List(
      Skipped,
      New,
      GeneratingTriples,
      GenerationRecoverableFailure,
      GenerationNonRecoverableFailure,
      TriplesGenerated,
      TransformingTriples,
      TransformationRecoverableFailure,
      TransformationNonRecoverableFailure,
      TriplesStore,
      AwaitingDeletion,
      Deleting
    )

    implicit val ordering: Ordering[EventStatus] = Ordering.by(statusesOrdered.indexOf)

    type New = New.type
    final case object New extends EventStatus {
      override val value: String = "NEW"
    }

    sealed trait ProcessingStatus extends EventStatus
    object ProcessingStatus {
      lazy val all: Set[ProcessingStatus] = Set(
        GeneratingTriples,
        TransformingTriples,
        Deleting
      )
    }

    type GeneratingTriples = GeneratingTriples.type
    final case object GeneratingTriples extends EventStatus with ProcessingStatus {
      override val value: String = "GENERATING_TRIPLES"
    }

    type TriplesGenerated = TriplesGenerated.type
    final case object TriplesGenerated extends EventStatus {
      override val value: String = "TRIPLES_GENERATED"
    }

    type TransformingTriples = TransformingTriples.type
    final case object TransformingTriples extends EventStatus with ProcessingStatus {
      override val value: String = "TRANSFORMING_TRIPLES"
    }

    type Deleting = Deleting.type
    final case object Deleting extends EventStatus with ProcessingStatus {
      override val value: String = "DELETING"
    }

    sealed trait FinalStatus extends EventStatus

    type TriplesStore = TriplesStore.type
    final case object TriplesStore extends EventStatus with FinalStatus {
      override val value: String = "TRIPLES_STORE"
    }
    final case object Skipped extends EventStatus with FinalStatus {
      override val value: String = "SKIPPED"
    }

    type AwaitingDeletion = AwaitingDeletion.type
    final case object AwaitingDeletion extends EventStatus with FinalStatus {
      override val value: String = "AWAITING_DELETION"
    }

    sealed trait FailureStatus extends EventStatus
    object FailureStatus {
      def fromString(str: String): Either[String, FailureStatus] = str.toUpperCase match {
        case GenerationRecoverableFailure.value        => GenerationRecoverableFailure.asRight
        case GenerationNonRecoverableFailure.value     => GenerationNonRecoverableFailure.asRight
        case TransformationRecoverableFailure.value    => TransformationRecoverableFailure.asRight
        case TransformationNonRecoverableFailure.value => TransformationNonRecoverableFailure.asRight
        case _                                         => Left(s"Invalid failure status: $str")
      }

      implicit val jsonDecoder: Decoder[FailureStatus] =
        Decoder.decodeString.emap(fromString)

      implicit val jsonEncoder: Encoder[FailureStatus] =
        Encoder.encodeString.contramap(_.value)
    }

    type GenerationRecoverableFailure = GenerationRecoverableFailure.type
    final case object GenerationRecoverableFailure extends FailureStatus {
      override val value: String = "GENERATION_RECOVERABLE_FAILURE"
    }

    type GenerationNonRecoverableFailure = GenerationNonRecoverableFailure.type
    final case object GenerationNonRecoverableFailure extends FailureStatus with FinalStatus {
      override val value: String = "GENERATION_NON_RECOVERABLE_FAILURE"
    }

    type TransformationRecoverableFailure = TransformationRecoverableFailure.type
    final case object TransformationRecoverableFailure extends FailureStatus {
      override val value: String = "TRANSFORMATION_RECOVERABLE_FAILURE"
    }

    type TransformationNonRecoverableFailure = TransformationNonRecoverableFailure.type
    final case object TransformationNonRecoverableFailure extends FailureStatus with FinalStatus {
      override val value: String = "TRANSFORMATION_NON_RECOVERABLE_FAILURE"
    }

    implicit val eventStatusJsonDecoder: Decoder[EventStatus] = decodeString.emap { value =>
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

  final case class EventStatusProgress(status: EventStatus) {
    lazy val stage:      EventStatusProgress.Stage      = EventStatusProgress.Stage(status)
    lazy val completion: EventStatusProgress.Completion = EventStatusProgress.Completion(stage)
  }

  object EventStatusProgress {
    import EventStatus._

    trait Stage extends IntTinyType with Product with Serializable
    object Stage {
      def apply(s: EventStatus): Stage = s match {
        case New                                                    => Initial
        case Skipped                                                => Final
        case GeneratingTriples | GenerationRecoverableFailure       => Generating
        case GenerationNonRecoverableFailure                        => Final
        case TriplesGenerated                                       => Generated
        case TransformingTriples | TransformationRecoverableFailure => Transforming
        case TransformationNonRecoverableFailure                    => Final
        case TriplesStore                                           => Final
        case AwaitingDeletion | Deleting                            => Removing
      }

      final case object Initial      extends Stage { override val value: Int = 1 }
      final case object Generating   extends Stage { override val value: Int = 2 }
      final case object Generated    extends Stage { override val value: Int = 3 }
      final case object Transforming extends Stage { override val value: Int = 4 }
      final case object Final        extends Stage { override val value: Int = 5 }
      final case object Removing     extends Stage { override val value: Int = -1 }

      implicit def show[S <: Stage]: Show[Stage] = Show.show {
        case Initial      => "Initial"
        case Generating   => "Generating"
        case Generated    => "Generated"
        case Transforming => "Transforming"
        case Final        => "Final"
        case Removing     => "Removing"
      }
    }

    final class Completion(val value: Float) extends AnyVal with FloatTinyType
    object Completion {
      def apply(stage: Stage): Completion = new Completion(
        stage match {
          case Stage.Removing => 0f
          case st =>
            BigDecimal((st.value.toDouble / Stage.Final.value.toDouble) * 100)
              .setScale(2, RoundingMode.HALF_DOWN)
              .toFloat
        }
      )
    }
  }

  final case class EventInfo(eventId:         EventId,
                             project:         ProjectIds,
                             status:          EventStatus,
                             eventDate:       EventDate,
                             executionDate:   ExecutionDate,
                             maybeMessage:    Option[EventMessage],
                             processingTimes: List[StatusProcessingTime]
  )

  final case class StatusProcessingTime(status: EventStatus, processingTime: EventProcessingTime)
  object StatusProcessingTime {
    implicit val jsonDecoder: Decoder[StatusProcessingTime] =
      Decoder.instance { cursor =>
        for {
          status <- cursor.downField("status").as[EventStatus]
          time   <- cursor.downField("processingTime").as[EventProcessingTime]
        } yield StatusProcessingTime(status, time)
      }
  }

  object EventInfo {
    final case class ProjectIds(id: projects.GitLabId, path: projects.Path)
    object ProjectIds {
      implicit val jsonDecoder: Decoder[ProjectIds] =
        io.circe.generic.semiauto.deriveDecoder[ProjectIds]

      implicit val show: Show[ProjectIds] =
        a => show"${a.id}/${a.path}"
    }

    implicit val show: Show[EventInfo] =
      Show.show(info => s"${info.eventId.show}/${info.project.show}/${info.status}")

    implicit val jsonDecoder: Decoder[EventInfo] =
      Decoder.instance { cursor =>
        for {
          id              <- cursor.downField("id").as[EventId]
          projectIds      <- cursor.downField("project").as[ProjectIds]
          status          <- cursor.downField("status").as[EventStatus]
          processingTimes <- cursor.downField("processingTimes").as[List[StatusProcessingTime]]
          date            <- cursor.downField("date").as[EventDate]
          executionDate   <- cursor.downField("executionDate").as[ExecutionDate]
          msg             <- cursor.downField("message").as[Option[EventMessage]]
        } yield EventInfo(id, projectIds, status, date, executionDate, msg, processingTimes)
      }
  }

  final class EventProcessingTime private (val value: Duration) extends AnyVal with DurationTinyType
  object EventProcessingTime
      extends TinyTypeFactory[EventProcessingTime](new EventProcessingTime(_))
      with DurationNotNegative[EventProcessingTime] {

    implicit val decoder: Decoder[EventProcessingTime] = durationDecoder(EventProcessingTime)

    implicit class EventProcessingTimeOps(processingTime: EventProcessingTime) {

      def *(multiplier: Int Refined Positive): EventProcessingTime =
        EventProcessingTime(Duration.ofMillis(processingTime.value.toMillis * multiplier.value))

      def /(multiplier: Int Refined Positive): EventProcessingTime =
        EventProcessingTime(Duration.ofMillis(processingTime.value.toMillis / multiplier.value))
    }
  }

  final class LastSyncedDate private (val value: Instant) extends AnyVal with InstantTinyType
  object LastSyncedDate
      extends TinyTypeFactory[LastSyncedDate](new LastSyncedDate(_))
      with InstantNotInTheFuture[LastSyncedDate] {
    implicit val decoder: Decoder[LastSyncedDate] = instantDecoder(LastSyncedDate)
  }
}
