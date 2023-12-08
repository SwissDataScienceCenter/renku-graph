package io.renku.eventlog

import cats.effect.IO
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import skunk._
import skunk.implicits._

trait EventLogDBFetching {
  self: EventLogPostgresSpec with TypeSerializers =>

  protected def findEvents(status: EventStatus, orderBy: Fragment[Void] = sql"created_date asc")(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[List[(CompoundEventId, ExecutionDate, BatchDate)]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventStatus *: Void *: EmptyTuple, (CompoundEventId, ExecutionDate, BatchDate)] = (sql"""
          SELECT event_id, project_id, execution_date, batch_date
          FROM event
          WHERE status = $eventStatusEncoder
          ORDER BY """ *: orderBy)
        .query(eventIdDecoder ~ projectIdDecoder ~ executionDateDecoder ~ batchDateDecoder)
        .map { case eventId ~ projectId ~ executionDate ~ batchDate =>
          (CompoundEventId(eventId, projectId), executionDate, batchDate)
        }
      session.prepare(query).flatMap(_.stream(status *: Void *: EmptyTuple, 32).compile.toList)
    }

  protected case class Event(executionDate: ExecutionDate, status: EventStatus, maybeMessage: Option[EventMessage])

  protected def findEvent(
      eventId: CompoundEventId
  )(implicit cfg: DBConfig[EventLogDB]): IO[Option[Event]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventId *: projects.GitLabId *: EmptyTuple, Event] =
        sql"""
          SELECT execution_date, status, message
          FROM event
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder"""
          .query(executionDateDecoder ~ eventStatusDecoder ~ eventMessageDecoder.opt)
          .map { case (executionDate: ExecutionDate) ~ (status: EventStatus) ~ (maybeMessage: Option[EventMessage]) =>
            Event(executionDate, status, maybeMessage)
          }
      session.prepare(query).flatMap(_.option(eventId.id *: eventId.projectId *: EmptyTuple))
    }
}
