package ch.datascience.triplesgenerator
package events.awaitinggeneration
import cats.MonadError
import cats.data.EitherT
import cats.effect.{Effect, IO}
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventId}
import ch.datascience.triplesgenerator.events.EventHandler.CategoryName
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult.{BadRequest, SchedulingError, UnsupportedEventType}

import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: Effect](
    eventsProcessingRunner: EventsProcessingRunner[Interpretation],
    eventBodyDeserialiser:  EventBodyDeserialiser[Interpretation],
    currentVersionPair:     RenkuVersionPair
)(implicit
    ME: MonadError[Interpretation, Throwable]
) extends events.EventHandler[Interpretation] {
  import EitherT.liftF
  import EventHandler._
  import currentVersionPair.schemaVersion
  import eventBodyDeserialiser.toCommitEvents
  import eventsProcessingRunner.scheduleForProcessing
  import org.http4s._
  import org.http4s.circe._

  override def handle(request: Request[Interpretation]): Interpretation[EventSchedulingResult] = {

    for {
      eventAndBody <- liftF(request.as[IdAndBody]) leftMap to(UnsupportedEventType)
      commitEvents <- liftF(toCommitEvents(eventAndBody._2)) leftMap to(BadRequest)
      result       <- liftF(scheduleForProcessing(eventAndBody._1, commitEvents, schemaVersion)) leftMap to(SchedulingError)

    } yield result
  }.fold(identity, identity)

  private def to(resultType: EventSchedulingResult): Throwable => EventSchedulingResult = { case NonFatal(_) =>
    resultType
  }

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, IdAndBody] = jsonOf[Interpretation, IdAndBody]

  override val name: CategoryName = CategoryName("AWAITING_GENERATION")
}

private[events] object EventHandler {
  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe.{Decoder, HCursor}

  type IdAndBody = (CompoundEventId, EventBody)

  implicit val eventDecoder: Decoder[IdAndBody] = (cursor: HCursor) =>
    for {
      id        <- cursor.downField("id").as[EventId]
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
      body      <- cursor.downField("body").as[EventBody]
    } yield CompoundEventId(id, projectId) -> body

  def apply(): IO[EventHandler[IO]] = IO {
    new EventHandler[IO](???, ???, ???)
  }
}
