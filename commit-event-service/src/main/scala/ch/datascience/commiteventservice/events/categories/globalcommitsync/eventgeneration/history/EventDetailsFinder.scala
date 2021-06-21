package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.history

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.CommitWithParents
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.projects
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[globalcommitsync] trait EventDetailsFinder[Interpretation[_]] {
  def findAllCommits(projectId: projects.Id): Interpretation[List[CommitWithParents]]

}

private[globalcommitsync] class EventDetailsFinderImpl[Interpretation[_]](
    eventLogUrl:             EventLogUrl,
    logger:                  Logger[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends EventDetailsFinder[Interpretation] {
  override def findAllCommits(projectId: projects.Id): Interpretation[List[CommitWithParents]] = ???
}

private[globalcommitsync] object EventDetailsFinder {
  def apply(
      logger: Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventDetailsFinder[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventDetailsFinderImpl[IO](eventLogUrl, logger)
}
