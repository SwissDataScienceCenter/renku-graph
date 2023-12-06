/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, TransformationNonRecoverableFailure, TransformationRecoverableFailure, TransformingTriples, TriplesGenerated, TriplesStore}
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import skunk._
import skunk.implicits._

import java.time.Instant

trait EventLogDBProvisioning {
  self: EventLogPostgresSpec with TypeSerializers =>

  protected def storeEvent(compoundEventId:   CompoundEventId,
                           eventStatus:       EventStatus,
                           executionDate:     ExecutionDate,
                           eventDate:         EventDate,
                           eventBody:         EventBody,
                           createdDate:       CreatedDate = CreatedDate(Instant.now),
                           batchDate:         BatchDate = BatchDate(Instant.now),
                           projectSlug:       projects.Slug = projectSlugs.generateOne,
                           maybeMessage:      Option[EventMessage] = None,
                           maybeEventPayload: Option[ZippedEventPayload] = None
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    upsertProject(compoundEventId.projectId, projectSlug, eventDate) >>
      insertEvent(compoundEventId,
                  eventStatus,
                  executionDate,
                  eventDate,
                  eventBody,
                  createdDate,
                  batchDate,
                  maybeMessage
      ) >>
      upsertEventPayload(compoundEventId, eventStatus, maybeEventPayload)

  protected def upsertProject(projectId: projects.GitLabId, projectSlug: projects.Slug, eventDate: EventDate)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[projects.GitLabId *: projects.Slug *: EventDate *: EmptyTuple] = sql"""
          INSERT INTO project (project_id, project_slug, latest_event_date)
          VALUES ($projectIdEncoder, $projectSlugEncoder, $eventDateEncoder)
          ON CONFLICT (project_id)
          DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
          """.command
      session.prepare(query).flatMap(_.execute(projectId *: projectSlug *: eventDate *: EmptyTuple)).void
    }

  protected def insertEvent(compoundEventId: CompoundEventId,
                            eventStatus:     EventStatus,
                            executionDate:   ExecutionDate,
                            eventDate:       EventDate,
                            eventBody:       EventBody,
                            createdDate:     CreatedDate,
                            batchDate:       BatchDate,
                            maybeMessage:    Option[EventMessage]
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      maybeMessage match {
        case None =>
          val query: Command[
            EventId *: projects.GitLabId *: EventStatus *: CreatedDate *: ExecutionDate *: EventDate *: BatchDate *: EventBody *: EmptyTuple
          ] = sql"""
            INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
            VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder)
            """.command
          session
            .prepare(query)
            .flatMap(
              _.execute(
                compoundEventId.id *: compoundEventId.projectId *: eventStatus *: createdDate *: executionDate *: eventDate *: batchDate *: eventBody *: EmptyTuple
              )
            )
            .void
        case Some(message) =>
          val query: Command[
            EventId *: projects.GitLabId *: EventStatus *: CreatedDate *: ExecutionDate *: EventDate *: BatchDate *: EventBody *: EventMessage *: EmptyTuple
          ] = sql"""
            INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
            VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder, $eventMessageEncoder)
            """.command
          session
            .prepare(query)
            .flatMap(
              _.execute(
                compoundEventId.id *: compoundEventId.projectId *: eventStatus *: createdDate *: executionDate *: eventDate *: batchDate *: eventBody *: message *: EmptyTuple
              )
            )
            .void
      }
    }

  protected def upsertEventPayload(compoundEventId: CompoundEventId,
                                   eventStatus:     EventStatus,
                                   maybePayload:    Option[ZippedEventPayload]
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    eventStatus match {
      case TriplesGenerated | TransformingTriples | TransformationRecoverableFailure |
          TransformationNonRecoverableFailure | TriplesStore | AwaitingDeletion =>
        maybePayload
          .map { payload =>
            moduleSessionResource(cfg).session.use { session =>
              val query: Command[EventId *: projects.GitLabId *: ZippedEventPayload *: EmptyTuple] = sql"""
                INSERT INTO event_payload (event_id, project_id, payload)
                VALUES ($eventIdEncoder, $projectIdEncoder, $zippedPayloadEncoder)
                ON CONFLICT (event_id, project_id)
                DO UPDATE SET payload = excluded.payload
              """.command
              session
                .prepare(query)
                .flatMap(_.execute(compoundEventId.id *: compoundEventId.projectId *: payload *: EmptyTuple))
                .void
            }
          }
          .getOrElse(().pure[IO])
      case _ => ().pure[IO]
    }
}
