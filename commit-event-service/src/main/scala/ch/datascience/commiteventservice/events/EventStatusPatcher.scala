package ch.datascience.commiteventservice.events

import cats.MonadThrow
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.control.Throttler
import ch.datascience.graph.config.EventLogUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.RestClient
import io.circe.literal.JsonStringContext
import org.http4s.Method.PATCH
import org.http4s.Status.Accepted
import org.http4s.multipart.{Multipart, Part}
import org.http4s.{Request, Response, Status}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait EventStatusPatcher[Interpretation[_]] {
  def sendDeletionStatus(eventId: CommitId, projectId: projects.Id): Interpretation[Unit]
}

private class EventStatusPatcherImpl[Interpretation[_]: MonadThrow: ConcurrentEffect: Timer](
    logger:                  Logger[Interpretation],
    eventLogUrl:             EventLogUrl
)(implicit executionContext: ExecutionContext)
    extends RestClient[Interpretation, EventStatusPatcher[Interpretation]](Throttler.noThrottling, logger)
    with EventStatusPatcher[Interpretation] {
  override def sendDeletionStatus(eventId: CommitId, projectId: projects.Id): Interpretation[Unit] = {
    val entity = Multipart[Interpretation](
      Vector(Part.formData[Interpretation]("event", json"""{"status": "AWAITING_DELETION"}""".noSpaces))
    )
    for {
      uri           <- validateUri(s"$eventLogUrl/events/$eventId/$projectId")
      sendingResult <- send(request(PATCH, uri).withEntity(entity))(mapResponse)
    } yield sendingResult
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[Interpretation], Response[Interpretation]), Interpretation[Unit]] = {
    case (Accepted, _, _) => ().pure[Interpretation]
  }

}

private object EventStatusPatcher {
  def apply(logger:     Logger[IO])(implicit
      executionContext: ExecutionContext,
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO]
  ): IO[EventStatusPatcherImpl[IO]] = for {
    eventLogUrl <- EventLogUrl[IO]()
  } yield new EventStatusPatcherImpl[IO](logger, eventLogUrl)
}
