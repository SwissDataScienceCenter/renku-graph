package io.renku.eventlog

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import org.scalatest.Suite
import skunk._
import skunk.implicits._

trait EventLogDBFetching {
  self: EventLogPostgresSpec with TypeSerializers with Suite =>

  protected def findEvents(status: EventStatus, orderBy: Fragment[Void] = sql"created_date asc")(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[List[FoundEvent]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventStatus *: Void *: EmptyTuple, FoundEvent] = (sql"""
          SELECT event_id, project_id, execution_date, batch_date
          FROM event
          WHERE status = $eventStatusEncoder
          ORDER BY """ *: orderBy)
        .query(eventIdDecoder ~ projectIdDecoder ~ executionDateDecoder ~ batchDateDecoder)
        .map {
          case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (executionDate: ExecutionDate) ~ (batchDate: BatchDate) =>
            FoundEvent(CompoundEventId(eventId, projectId), executionDate, status, batchDate)
        }
      session.prepare(query).flatMap(_.stream(status *: Void *: EmptyTuple, 32).compile.toList)
    }

  protected case class FoundEvent(maybeId:            Option[CompoundEventId],
                                  maybeExecutionDate: Option[ExecutionDate],
                                  maybeStatus:        Option[EventStatus],
                                  maybeMaybeMessage:  Option[Option[EventMessage]],
                                  maybeBatchDate:     Option[BatchDate]
  ) {

    lazy val id:     CompoundEventId = maybeId.getOrElse(fail("expected id on FoundEvent"))
    lazy val status: EventStatus     = maybeStatus.getOrElse(fail("expected status on FoundEvent"))

    def select(first: Field, other: Field*): FoundEvent = {
      val allFields = first :: other.toList
      def clearOrSet[V](orig: Option[V], field: Field): Option[V] =
        if (allFields contains field) orig else Option.empty[V]
      FoundEvent(
        clearOrSet(maybeId, Field.Id),
        clearOrSet(maybeExecutionDate, Field.ExecutionDate),
        clearOrSet(maybeStatus, Field.Status),
        clearOrSet(maybeMaybeMessage, Field.Message),
        clearOrSet(maybeBatchDate, Field.BatchDate)
      )
    }
  }

  protected trait Field extends Product
  protected object Field {
    case object Id            extends Field
    case object ExecutionDate extends Field
    case object Status        extends Field
    case object Message       extends Field
    case object BatchDate     extends Field
  }

  protected object FoundEvent {

    def apply(id:            CompoundEventId,
              executionDate: ExecutionDate,
              status:        EventStatus,
              maybeMessage:  Option[EventMessage]
    ): FoundEvent =
      FoundEvent(id.some, executionDate.some, status.some, maybeMessage.some, maybeBatchDate = None)

    def apply(id:            CompoundEventId,
              executionDate: ExecutionDate,
              status:        EventStatus,
              batchDate:     BatchDate
    ): FoundEvent =
      FoundEvent(id.some, executionDate.some, status.some, maybeMaybeMessage = None, batchDate.some)

    def apply(id: CompoundEventId): FoundEvent =
      FoundEvent(id.some, None, None, None, None)

    def apply(id: CompoundEventId, executionDate: ExecutionDate): FoundEvent =
      FoundEvent(id.some, executionDate.some, maybeStatus = None, maybeMaybeMessage = None, maybeBatchDate = None)

    def apply(status: EventStatus, maybeMessage: Option[EventMessage]): FoundEvent =
      FoundEvent(maybeId = None, maybeExecutionDate = None, status.some, maybeMessage.some, maybeBatchDate = None)
  }

  protected def findEvent(
      eventId: CompoundEventId
  )(implicit cfg: DBConfig[EventLogDB]): IO[Option[FoundEvent]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventId *: projects.GitLabId *: EmptyTuple, FoundEvent] = sql"""
          SELECT execution_date, status, message
          FROM event
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder"""
        .query(executionDateDecoder ~ eventStatusDecoder ~ eventMessageDecoder.opt)
        .map { case (executionDate: ExecutionDate) ~ (status: EventStatus) ~ (maybeMessage: Option[EventMessage]) =>
          FoundEvent(eventId, executionDate, status, maybeMessage)
        }
      session.prepare(query).flatMap(_.option(eventId.id *: eventId.projectId *: EmptyTuple))
    }
}
