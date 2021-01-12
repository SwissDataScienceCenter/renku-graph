package ch.datascience.triplesgenerator
package events.awaitinggeneration
import cats.MonadError
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{Effect, IO}
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.events.{CompoundEventId, EventBody, EventId}
import ch.datascience.triplesgenerator.events.EventHandler.CategoryName
import ch.datascience.triplesgenerator.events.EventSchedulingResult
import ch.datascience.triplesgenerator.events.EventSchedulingResult._
import io.chrisdavenport.log4cats.Logger
import io.circe.DecodingFailure

import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: Effect](
    eventsProcessingRunner: EventsProcessingRunner[Interpretation],
    eventBodyDeserialiser:  EventBodyDeserialiser[Interpretation],
    currentVersionPair:     RenkuVersionPair,
    logger:                 Logger[Interpretation]
)(implicit
    ME: MonadError[Interpretation, Throwable]
) extends events.EventHandler[Interpretation] {

  import EitherT._
  import cats.syntax.all._
  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import currentVersionPair.schemaVersion
  import eventBodyDeserialiser.toCommitEvents
  import eventsProcessingRunner.scheduleForProcessing
  import io.circe.{Decoder, HCursor}
  import org.http4s._
  import org.http4s.circe._

  type IdAndBody = (CompoundEventId, EventBody)

  override val name: CategoryName = CategoryName("AWAITING_GENERATION")

  override def handle(request: Request[Interpretation]): Interpretation[EventSchedulingResult] = {
    for {
      eventAndBody <-
        EitherT(request.as[IdAndBody].map(_.asRight[EventSchedulingResult]).recover(as(UnsupportedEventType)))
      (eventId, eventBody) = eventAndBody
      commitEvents <- liftF(toCommitEvents(eventBody)) leftMap to(BadRequest)
      result       <- liftF(scheduleForProcessing(eventId, commitEvents, schemaVersion)) leftMap to(SchedulingError)
      _ = maybeLog(eventId, commitEvents, result)
    } yield result
  }.fold(identity, identity)

  private def as(
      resultType: EventSchedulingResult
  ): PartialFunction[Throwable, Either[EventSchedulingResult, IdAndBody]] = { case NonFatal(_) =>
    Left(resultType)
  }

  private def to(resultType: EventSchedulingResult): Throwable => EventSchedulingResult = { case NonFatal(_) =>
    resultType
  }

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, IdAndBody] = jsonOf[Interpretation, IdAndBody]

  private def maybeLog(eventId:      CompoundEventId,
                       commitEvents: NonEmptyList[CommitEvent],
                       result:       EventSchedulingResult
  ): Interpretation[Unit] = result match {
    case Accepted => logger.info(s"$name: $eventId, projectPath = ${commitEvents.head.project.path} -> $result")
    case _        => ME.unit
  }

  private implicit val eventDecoder: Decoder[IdAndBody] = (cursor: HCursor) =>
    for {
      _         <- cursor.downField("categoryName").as[CategoryName] flatMap checkCategoryName
      id        <- cursor.downField("id").as[EventId]
      projectId <- cursor.downField("project").downField("id").as[projects.Id]
      body      <- cursor.downField("body").as[EventBody]
    } yield CompoundEventId(id, projectId) -> body

  private lazy val checkCategoryName: CategoryName => Decoder.Result[CategoryName] = {
    case name @ `name` => Right(name)
    case other         => Left(DecodingFailure(s"$other not suppoerted by $name", Nil))
  }
}

private[events] object EventHandler {

  def apply(): IO[EventHandler[IO]] = IO {
    new EventHandler[IO](???, ???, ???, ???)
  }
}
