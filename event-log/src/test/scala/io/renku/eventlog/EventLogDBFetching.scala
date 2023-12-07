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

  protected def findEvent(
      eventId: CompoundEventId
  )(implicit cfg: DBConfig[EventLogDB]): IO[Option[(ExecutionDate, EventStatus, Option[EventMessage])]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventId *: projects.GitLabId *: EmptyTuple, (ExecutionDate, EventStatus, Option[EventMessage])] =
        sql"""
          SELECT execution_date, status, message
          FROM event
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder"""
          .query(executionDateDecoder ~ eventStatusDecoder ~ eventMessageDecoder.opt)
          .map { case executionDate ~ status ~ maybeMessage => (executionDate, status, maybeMessage) }
      session.prepare(query).flatMap(_.option(eventId.id *: eventId.projectId *: EmptyTuple))
    }
}
