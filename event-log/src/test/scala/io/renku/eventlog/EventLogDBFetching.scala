/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.eventlog

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.producers.eventdelivery.EventTypeId
import io.renku.events.Subscription.SubscriberId
import io.renku.events.consumers.Project
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
          SELECT event_id, project_id, execution_date, execution_date, event_date, event_body, batch_date, message
          FROM event
          WHERE status = $eventStatusEncoder
          ORDER BY """ *: orderBy)
        .query(
          eventIdDecoder ~ projectIdDecoder ~ executionDateDecoder ~ createdDateDecoder ~ eventDateDecoder ~ eventBodyDecoder ~ batchDateDecoder ~ eventMessageDecoder.opt
        )
        .map {
          case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (executionDate: ExecutionDate) ~ (createdDate: CreatedDate) ~ (eventDate: EventDate) ~ (body: EventBody) ~ (batchDate: BatchDate) ~ (maybeMessage: Option[
                EventMessage
              ]) =>
            FoundEvent(CompoundEventId(eventId, projectId),
                       executionDate,
                       createdDate,
                       eventDate,
                       status,
                       body,
                       batchDate,
                       maybeMessage
            )
        }
      session.prepare(query).flatMap(_.stream(status *: Void *: EmptyTuple, 32).compile.toList)
    }

  protected case class FoundEvent(maybeId:            Option[CompoundEventId],
                                  maybeExecutionDate: Option[ExecutionDate],
                                  maybeCreatedDate:   Option[CreatedDate],
                                  maybeEventDate:     Option[EventDate],
                                  maybeStatus:        Option[EventStatus],
                                  maybeBody:          Option[EventBody],
                                  maybeMaybeMessage:  Option[Option[EventMessage]],
                                  maybeBatchDate:     Option[BatchDate]
  ) {

    lazy val id:           CompoundEventId      = maybeId.getOrElse(fail("expected id on FoundEvent"))
    lazy val status:       EventStatus          = maybeStatus.getOrElse(fail("expected status on FoundEvent"))
    lazy val maybeMessage: Option[EventMessage] = maybeMaybeMessage.getOrElse(fail("expected message on FoundEvent"))

    def select(first: Field, other: Field*): FoundEvent = {
      val allFields = first :: other.toList
      def setOrClear[V](orig: Option[V], field: Field): Option[V] =
        if (allFields contains field) orig else Option.empty[V]
      FoundEvent(
        setOrClear(maybeId, Field.Id),
        setOrClear(maybeExecutionDate, Field.ExecutionDate),
        setOrClear(maybeCreatedDate, Field.CreatedDate),
        setOrClear(maybeEventDate, Field.EventDate),
        setOrClear(maybeStatus, Field.Status),
        setOrClear(maybeBody, Field.Body),
        setOrClear(maybeMaybeMessage, Field.Message),
        setOrClear(maybeBatchDate, Field.BatchDate)
      )
    }

    def unselect(first: Field, other: Field*): FoundEvent = {
      val allFields = first :: other.toList
      def clearOrSet[V](orig: Option[V], field: Field): Option[V] =
        if (allFields contains field) Option.empty[V] else orig
      FoundEvent(
        clearOrSet(maybeId, Field.Id),
        clearOrSet(maybeExecutionDate, Field.ExecutionDate),
        clearOrSet(maybeCreatedDate, Field.CreatedDate),
        clearOrSet(maybeEventDate, Field.EventDate),
        clearOrSet(maybeStatus, Field.Status),
        clearOrSet(maybeBody, Field.Body),
        clearOrSet(maybeMaybeMessage, Field.Message),
        clearOrSet(maybeBatchDate, Field.BatchDate)
      )
    }
  }

  protected trait Field extends Product
  protected object Field {
    case object Id            extends Field
    case object CreatedDate   extends Field
    case object ExecutionDate extends Field
    case object EventDate     extends Field
    case object Body          extends Field
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
      FoundEvent(id.some, executionDate.some, None, None, status.some, None, maybeMessage.some, maybeBatchDate = None)

    def apply(id:            CompoundEventId,
              executionDate: ExecutionDate,
              createdDate:   CreatedDate,
              eventDate:     EventDate,
              status:        EventStatus,
              body:          EventBody,
              batchDate:     BatchDate,
              maybeMessage:  Option[EventMessage]
    ): FoundEvent = FoundEvent(id.some,
                               executionDate.some,
                               createdDate.some,
                               eventDate.some,
                               status.some,
                               body.some,
                               maybeMaybeMessage = maybeMessage.some,
                               batchDate.some
    )

    def apply(id: CompoundEventId): FoundEvent =
      FoundEvent(id.some, None, None, None, None, None, None, None)

    def apply(id: CompoundEventId, executionDate: ExecutionDate): FoundEvent =
      FoundEvent(id.some,
                 executionDate.some,
                 maybeCreatedDate = None,
                 maybeEventDate = None,
                 maybeStatus = None,
                 maybeBody = None,
                 maybeMaybeMessage = None,
                 maybeBatchDate = None
      )

    def apply(id: CompoundEventId, executionDate: ExecutionDate, batchDate: BatchDate): FoundEvent =
      FoundEvent(
        id.some,
        executionDate.some,
        maybeCreatedDate = None,
        maybeEventDate = None,
        maybeStatus = None,
        maybeBody = None,
        maybeMaybeMessage = None,
        maybeBatchDate = batchDate.some
      )

    def apply(status: EventStatus, executionDate: ExecutionDate, maybeMessage: Option[EventMessage]): FoundEvent =
      FoundEvent(
        maybeId = None,
        maybeExecutionDate = executionDate.some,
        maybeCreatedDate = None,
        maybeEventDate = None,
        maybeStatus = status.some,
        maybeBody = None,
        maybeMaybeMessage = maybeMessage.some,
        maybeBatchDate = None
      )

    def apply(status: EventStatus, maybeMessage: Option[EventMessage]): FoundEvent =
      FoundEvent(
        maybeId = None,
        maybeExecutionDate = None,
        maybeCreatedDate = None,
        maybeEventDate = None,
        maybeStatus = status.some,
        maybeBody = None,
        maybeMaybeMessage = maybeMessage.some,
        maybeBatchDate = None
      )
  }

  protected def findEvent(
      eventId: CompoundEventId
  )(implicit cfg: DBConfig[EventLogDB]): IO[Option[FoundEvent]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventId *: projects.GitLabId *: EmptyTuple, FoundEvent] = sql"""
          SELECT execution_date, created_date, event_date, status, event_body, batch_date, message
          FROM event
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder"""
        .query(
          executionDateDecoder ~ createdDateDecoder ~ eventDateDecoder ~ eventStatusDecoder ~ eventBodyDecoder ~ batchDateDecoder ~ eventMessageDecoder.opt
        )
        .map {
          case (executionDate: ExecutionDate) ~ (createdDate: CreatedDate) ~ (eventDate: EventDate) ~ (status: EventStatus) ~ (body: EventBody) ~ (batchDate: BatchDate) ~
              (maybeMessage: Option[EventMessage]) =>
            FoundEvent(eventId, executionDate, createdDate, eventDate, status, body, batchDate, maybeMessage)
        }
      session.prepare(query).flatMap(_.option(eventId.id *: eventId.projectId *: EmptyTuple))
    }

  protected case class FoundPayload(eventId: CompoundEventId, payload: ZippedEventPayload)

  protected def findPayload(eventId: CompoundEventId)(implicit cfg: DBConfig[EventLogDB]): IO[Option[FoundPayload]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventId *: projects.GitLabId *: EmptyTuple, FoundPayload] = sql"""
          SELECT event_id, project_id, payload
          FROM event_payload
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder"""
        .query(eventIdDecoder ~ projectIdDecoder ~ zippedPayloadDecoder)
        .map { case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (payload: ZippedEventPayload) =>
          FoundPayload(CompoundEventId(eventId, projectId), payload)
        }
      session.prepare(query).flatMap(_.option(eventId.id *: eventId.projectId *: EmptyTuple))
    }

  protected case class ProcessingTime(eventId: CompoundEventId, processingTime: EventProcessingTime)

  protected def findProcessingTimes(
      eventId: CompoundEventId
  )(implicit cfg: DBConfig[EventLogDB]): IO[List[ProcessingTime]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[EventId *: projects.GitLabId *: EmptyTuple, ProcessingTime] = sql"""
          SELECT event_id, project_id, processing_time
          FROM status_processing_time
          WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder;
        """
        .query(eventIdDecoder ~ projectIdDecoder ~ eventProcessingTimeDecoder)
        .map { case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (processingTime: EventProcessingTime) =>
          ProcessingTime(CompoundEventId(eventId, projectId), processingTime)
        }
      session.prepare(query).flatMap(_.stream(eventId.id *: eventId.projectId *: EmptyTuple, 32).compile.toList)
    }

  protected def findProcessingTimes(
      projectId: projects.GitLabId
  )(implicit cfg: DBConfig[EventLogDB]): IO[List[ProcessingTime]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[projects.GitLabId, ProcessingTime] = sql"""
          SELECT event_id, project_id, processing_time
          FROM status_processing_time
          WHERE project_id = $projectIdEncoder"""
        .query(eventIdDecoder ~ projectIdDecoder ~ eventProcessingTimeDecoder)
        .map { case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (processingTime: EventProcessingTime) =>
          ProcessingTime(CompoundEventId(eventId, projectId), processingTime)
        }
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
    }

  protected case class FoundDelivery(eventId: CompoundEventId, subscriberId: SubscriberId)

  protected object FoundDelivery {
    def apply(event: GeneratedEvent, subscriberId: SubscriberId): FoundDelivery =
      FoundDelivery(event.eventId, subscriberId)
  }

  protected def findAllEventDeliveries(implicit cfg: DBConfig[EventLogDB]): IO[List[FoundDelivery]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[Void, FoundDelivery] =
        sql"""SELECT event_id, project_id, delivery_id
              FROM event_delivery WHERE event_id IS NOT NULL"""
          .query(eventIdDecoder ~ projectIdDecoder ~ subscriberIdDecoder)
          .map { case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (subscriberId: SubscriberId) =>
            FoundDelivery(CompoundEventId(eventId, projectId), subscriberId)
          }

      session.execute(query)
    }

  protected case class FoundProjectDelivery(projectId:    projects.GitLabId,
                                            subscriberId: SubscriberId,
                                            eventTypeId:  EventTypeId
  )

  protected def findAllProjectDeliveries(implicit cfg: DBConfig[EventLogDB]): IO[List[FoundProjectDelivery]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[Void, FoundProjectDelivery] =
        sql"""SELECT project_id, delivery_id, event_type_id
              FROM event_delivery
              WHERE event_id IS NULL"""
          .query(projectIdDecoder ~ subscriberIdDecoder ~ eventTypeIdDecoder)
          .map { case (projectId: projects.GitLabId) ~ (subscriberId: SubscriberId) ~ (eventTypeId: EventTypeId) =>
            FoundProjectDelivery(projectId, subscriberId, eventTypeId)
          }

      session.execute(query)
    }

  protected case class FoundProject(project: Project, eventDate: EventDate)

  protected def findProjects(implicit cfg: DBConfig[EventLogDB]): IO[List[FoundProject]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[Void, FoundProject] =
        sql"""SELECT * FROM project"""
          .query(projectIdDecoder ~ projectSlugDecoder ~ eventDateDecoder)
          .map { case (projectId: projects.GitLabId) ~ (projectSlug: projects.Slug) ~ (eventDate: EventDate) =>
            FoundProject(Project(projectId, projectSlug), eventDate)
          }
      session.execute(query)
    }

  protected def findAllProjectEvents(
      projectId: projects.GitLabId
  )(implicit cfg: DBConfig[EventLogDB]): IO[List[CompoundEventId]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[projects.GitLabId, CompoundEventId] = sql"""
          SELECT event_id, project_id
          FROM event
          WHERE project_id = $projectIdEncoder
          ORDER BY event_date"""
        .query(eventIdDecoder ~ projectIdDecoder)
        .map { case eventId ~ projectId => CompoundEventId(eventId, projectId) }
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
    }

  protected case class FoundProjectPayload(eventId: CompoundEventId, payload: ZippedEventPayload)

  protected def findAllProjectPayloads(
      projectId: projects.GitLabId
  )(implicit cfg: DBConfig[EventLogDB]): IO[List[FoundProjectPayload]] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Query[projects.GitLabId, FoundProjectPayload] = sql"""
            SELECT event_id, project_id, payload
            FROM event_payload
            WHERE project_id = $projectIdEncoder"""
        .query(eventIdDecoder ~ projectIdDecoder ~ zippedPayloadDecoder)
        .map { case (eventId: EventId) ~ (projectId: projects.GitLabId) ~ (eventPayload: ZippedEventPayload) =>
          FoundProjectPayload(CompoundEventId(eventId, projectId), eventPayload)
        }
      session.prepare(query).flatMap(_.stream(projectId, 32).compile.toList)
    }
}
