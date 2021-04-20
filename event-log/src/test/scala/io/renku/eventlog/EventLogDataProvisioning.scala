/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.data.Kleisli
import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl, subscriberIds, subscriberUrls}
import ch.datascience.generators.CommonGraphGenerators.microserviceBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, projectSchemaVersions}
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.graph.model.events.EventStatus.{TransformationRecoverableFailure, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects.Path
import ch.datascience.microservices.MicroserviceBaseUrl
import skunk._
import skunk.implicits._

import java.time.Instant

trait EventLogDataProvisioning {
  self: InMemoryEventLogDb =>

  protected def storeEvent(compoundEventId:      CompoundEventId,
                           eventStatus:          EventStatus,
                           executionDate:        ExecutionDate,
                           eventDate:            EventDate,
                           eventBody:            EventBody,
                           createdDate:          CreatedDate = CreatedDate(Instant.now),
                           batchDate:            BatchDate = BatchDate(Instant.now),
                           projectPath:          Path = projectPaths.generateOne,
                           maybeMessage:         Option[EventMessage] = None,
                           payloadSchemaVersion: SchemaVersion = projectSchemaVersions.generateOne,
                           maybeEventPayload:    Option[EventPayload] = None
  ): Unit = {
    upsertProject(compoundEventId, projectPath, eventDate)
    insertEvent(compoundEventId, eventStatus, executionDate, eventDate, eventBody, createdDate, batchDate, maybeMessage)
    upsertEventPayload(compoundEventId, eventStatus, payloadSchemaVersion, maybeEventPayload)
  }

  protected def insertEvent(compoundEventId: CompoundEventId,
                            eventStatus:     EventStatus,
                            executionDate:   ExecutionDate,
                            eventDate:       EventDate,
                            eventBody:       EventBody,
                            createdDate:     CreatedDate,
                            batchDate:       BatchDate,
                            maybeMessage:    Option[EventMessage]
  ): Unit = execute {
    Kleisli { session =>
      maybeMessage match {
        case None =>
          val query: Command[
            EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody
          ] =
            sql"""INSERT INTO
                  event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
                  VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder)
        """.command
          session
            .prepare(query)
            .use(
              _.execute(
                compoundEventId.id ~ compoundEventId.projectId ~ eventStatus ~ createdDate ~ executionDate ~ eventDate ~ batchDate ~ eventBody
              )
            )
            .void
        case Some(message) =>
          val query: Command[
            EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody ~ EventMessage
          ] =
            sql"""INSERT INTO
                  event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
                  VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder, $eventMessageEncoder)
               """.command
          session
            .prepare(query)
            .use(
              _.execute(
                compoundEventId.id ~ compoundEventId.projectId ~ eventStatus ~ createdDate ~ executionDate ~ eventDate ~ batchDate ~ eventBody ~ message
              )
            )
            .void
      }
    }
  }

  protected def upsertProject(compoundEventId: CompoundEventId, projectPath: Path, eventDate: EventDate): Unit =
    upsertProject(compoundEventId.projectId, projectPath, eventDate)

  protected def upsertProject(projectId: projects.Id, projectPath: Path, eventDate: EventDate): Unit =
    execute {
      Kleisli { session =>
        val query: Command[projects.Id ~ projects.Path ~ EventDate] =
          sql"""
            INSERT INTO
            project (project_id, project_path, latest_event_date)
            VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
            ON CONFLICT (project_id)
            DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
      """.command
        session.prepare(query).use(_.execute(projectId ~ projectPath ~ eventDate)).void
      }
    }

  protected def upsertEventPayload(compoundEventId: CompoundEventId,
                                   eventStatus:     EventStatus,
                                   schemaVersion:   SchemaVersion,
                                   maybePayload:    Option[EventPayload]
  ): Unit = (eventStatus, maybePayload) match {
    case (TriplesGenerated | TransformationRecoverableFailure | TransformingTriples, Some(payload)) =>
      execute[Unit] {
        Kleisli { session =>
          val query: Command[EventId ~ projects.Id ~ EventPayload ~ SchemaVersion] =
            sql"""
              INSERT INTO
              event_payload (event_id, project_id, payload, schema_version)
              VALUES ($eventIdEncoder, $projectIdEncoder, $eventPayloadEncoder, $schemaVersionEncoder)
              ON CONFLICT (event_id, project_id, schema_version)
              DO UPDATE SET payload = excluded.payload
      """.command
          session
            .prepare(query)
            .use(_.execute(compoundEventId.id ~ compoundEventId.projectId ~ payload ~ schemaVersion))
            .void
        }
      }
    case _ => ()
  }

  protected def upsertProcessingTime(compoundEventId: CompoundEventId,
                                     eventStatus:     EventStatus,
                                     processingTime:  EventProcessingTime
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[EventId ~ projects.Id ~ EventStatus ~ EventProcessingTime] =
        sql"""
          INSERT INTO
          status_processing_time (event_id, project_id, status, processing_time)
          VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $eventProcessingTimeEncoder)
          ON CONFLICT (event_id, project_id, status)
          DO UPDATE SET processing_time = excluded.processing_time
      """.command
      session
        .prepare(query)
        .use(_.execute(compoundEventId.id ~ compoundEventId.projectId ~ eventStatus ~ processingTime))
        .void
    }
  }

  protected def upsertEventDeliveryInfo(
      eventId:     CompoundEventId,
      deliveryId:  SubscriberId = subscriberIds.generateOne,
      deliveryUrl: SubscriberUrl = subscriberUrls.generateOne,
      sourceUrl:   MicroserviceBaseUrl = microserviceBaseUrls.generateOne
  ): Unit = {
    upsertSubscriber(deliveryId, deliveryUrl, sourceUrl)
    upsertEventDelivery(eventId, deliveryId)
  }

  protected def upsertSubscriber(deliveryId:  SubscriberId,
                                 deliveryUrl: SubscriberUrl,
                                 sourceUrl:   MicroserviceBaseUrl
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[SubscriberId ~ SubscriberUrl ~ MicroserviceBaseUrl ~ SubscriberId] =
        sql"""
        INSERT INTO
        subscriber (delivery_id, delivery_url, source_url)
        VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
        ON CONFLICT (delivery_url, source_url)
        DO UPDATE SET delivery_id = $subscriberIdEncoder, delivery_url = EXCLUDED.delivery_url, source_url = EXCLUDED.source_url
      """.command

      session.prepare(query).use(_.execute(deliveryId ~ deliveryUrl ~ sourceUrl ~ deliveryId)).void
    }
  }

  protected def upsertEventDelivery(eventId:    CompoundEventId,
                                    deliveryId: SubscriberId = subscriberIds.generateOne
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[EventId ~ projects.Id ~ SubscriberId] =
        sql"""
        INSERT INTO
        event_delivery (event_id, project_id, delivery_id)
        VALUES ($eventIdEncoder, $projectIdEncoder, $subscriberIdEncoder)
        ON CONFLICT (event_id, project_id)
        DO NOTHING
    """.command
      session.prepare(query).use(_.execute(eventId.id ~ eventId.projectId ~ deliveryId)).void
    }
  }

  protected def findAllDeliveries: List[(CompoundEventId, SubscriberId)] = execute {
    Kleisli { session =>
      val query: Query[Void, (CompoundEventId, SubscriberId)] =
        sql"""
          SELECT event_id, project_id, delivery_id
          FROM event_delivery"""
          .query(eventIdDecoder ~ projectIdDecoder ~ subscriberIdDecoder)
          .map { case eventId ~ projectId ~ subscriberId => (CompoundEventId(eventId, projectId), subscriberId) }
      session.execute(query)
    }
  }
}
