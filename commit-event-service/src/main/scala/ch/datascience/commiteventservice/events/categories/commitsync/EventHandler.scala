package ch.datascience.commiteventservice.events.categories.commitsync

import cats.MonadError
import cats.data.EitherT.fromEither
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult, Project}
import ch.datascience.graph.model.events.{BatchDate, CategoryName, CommitId, EventBody, EventId, EventStatus, LastSyncedDate}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, DecodingFailure, HCursor}

import scala.concurrent.ExecutionContext

private class EventHandler[Interpretation[_]](
    override val categoryName: CategoryName,
    missedEventLoader:         MissedEventsLoader[Interpretation],
    logger:                    Logger[Interpretation]
)(implicit
    ME: MonadError[Interpretation, Throwable]
) extends consumers.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import missedEventLoader._

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      _ <- fromEither[Interpretation](request.event.validateCategoryName)
      event <-
        fromEither[Interpretation](
          request.event.as[CommitSyncEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
        )
      result <- loadMissedEvents(event).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(event))
                  .leftSemiflatTap(logger.log(event))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: CommitSyncEvent => String = { event =>
    s"${event.id}, projectPath = ${event.project.path}, lastSynced = ${event.lastSynced}"
  }

  private implicit val eventDecoder: Decoder[CommitSyncEvent] = (cursor: HCursor) =>
    cursor.downField("status").as[Option[String]] flatMap {
      case "COMMIT_SYNC" =>
        for {
          id          <- cursor.downField("id").as[CommitId]
          projectId   <- cursor.downField("project").as[projects.Id]
          projectPath <- cursor.downField("project").as[projects.Path]
          lastSynced  <- cursor.downField("id").as[LastSyncedDate]

        } yield CommitSyncEvent(categoryName, id, CommitProject(projectId, projectPath), lastSynced)

      case Some(invalidStatus) =>
        Left(DecodingFailure(s"Status $invalidStatus is not valid. Only NEW or SKIPPED are accepted", Nil))
    }

//  private def verifyMessage(result: Decoder.Result[Option[String]]) =
//    result
//      .map(blankToNone)
//      .flatMap {
//        case None          => Left(DecodingFailure(s"Skipped Status requires message", Nil))
//        case Some(message) => EventMessage.from(message.value)
//      }
//      .leftMap(_ => DecodingFailure("Invalid Skipped Event message", Nil))

  implicit val projectDecoder: Decoder[Project] = (cursor: HCursor) =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)
}

private object EventHandler {
  def apply(waitingEventsGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name],
            logger:             Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    missedEventsLoader <- IOMissedEventsLoader(gitLabThrottler = ???, executionTimeRecorder = ???, logger = logger)
  } yield new EventHandler[IO](CategoryName("COMMIT_SYNC"), missedEventsLoader, logger)
}
