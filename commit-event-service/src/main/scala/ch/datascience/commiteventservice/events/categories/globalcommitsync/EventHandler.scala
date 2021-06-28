package ch.datascience.commiteventservice.events.categories.globalcommitsync

import cats.MonadThrow
import cats.data.EitherT.fromEither
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.GlobalCommitEventSynchronizer
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest}
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult, Project}
import ch.datascience.graph.model.events.{CategoryName, LastSyncedDate}
import ch.datascience.logging.ExecutionTimeRecorder
import io.circe.Decoder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[events] class EventHandler[Interpretation[_]: MonadThrow](
    override val categoryName: CategoryName,
    commitEventSynchronizer:   GlobalCommitEventSynchronizer[Interpretation],
    logger:                    Logger[Interpretation]
)(implicit
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends consumers.EventHandler[Interpretation] {
  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import commitEventSynchronizer._

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = {
    for {
      _ <- fromEither[Interpretation](request.event.validateCategoryName)
      event <-
        fromEither[Interpretation](
          request.event.as[GlobalCommitSyncEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
        )
      result <- (contextShift.shift *> concurrent
                  .start(synchronizeEvents(event) recoverWith logError(event))).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger log event)
                  .leftSemiflatTap(logger log event)
    } yield result
  }.merge

  private implicit val eventDecoder: Decoder[GlobalCommitSyncEvent] =
    cursor =>
      for {
        project    <- cursor.downField("project").as[Project]
        lastSynced <- cursor.downField("lastSynced").as[LastSyncedDate]
      } yield GlobalCommitSyncEvent(project, lastSynced)

  private implicit lazy val projectDecoder: Decoder[Project] = cursor =>
    for {
      id   <- cursor.downField("id").as[projects.Id]
      path <- cursor.downField("path").as[projects.Path]
    } yield Project(id, path)

  private implicit lazy val eventInfoToString: GlobalCommitSyncEvent => String = _.toString

  private def logError(event: GlobalCommitSyncEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.logError(event, exception)
      exception.raiseError[Interpretation, Unit]
  }
}

private[events] object EventHandler {
  def apply(
      gitLabThrottler:       Throttler[IO, GitLab],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    globalCommitEventSynchronizer <- GlobalCommitEventSynchronizer(gitLabThrottler, executionTimeRecorder, logger)
  } yield new EventHandler[IO](categoryName, globalCommitEventSynchronizer, logger)
}
