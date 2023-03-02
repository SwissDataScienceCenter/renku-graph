package io.renku.eventlog.events.consumers.statuschange

import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import io.renku.graph.model.events.{EventId, EventMessage, EventProcessingTime, EventStatus}
import io.renku.graph.model.projects.{GitLabId, Path}
import java.time.{Duration => JDuration}

final case class RawStatusChangeEvent(
    id:             Option[EventId],
    project:        Option[RawStatusChangeEvent.Project],
    processingTime: Option[EventProcessingTime],
    message:        Option[EventMessage],
    executionDelay: Option[JDuration],
    newStatus:      EventStatus
)

object RawStatusChangeEvent {
  import io.renku.tinytypes.json.TinyTypeDecoders._

  implicit val gitlabIdDecoder: Decoder[GitLabId] =
    Decoder.decodeInt.emap(n => GitLabId.from(n).leftMap(_.getMessage))
  implicit val projectPathDecoder: Decoder[Path] =
    Decoder.decodeString.emap(s => Path.from(s).leftMap(_.getMessage))

  case class Project(id: Option[GitLabId], path: Path)

  object Project {
    implicit val decoder: Decoder[Project] = deriveDecoder[Project]
    implicit val encoder: Encoder[Project] = deriveEncoder[Project].mapJson(_.deepDropNullValues)
  }

  implicit val decoder: Decoder[RawStatusChangeEvent] = deriveDecoder[RawStatusChangeEvent]
  implicit val encoder: Encoder[RawStatusChangeEvent] = deriveEncoder[RawStatusChangeEvent]
}
