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

import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, projectSchemaVersions}
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventStatus.{TransformationRecoverableFailure, TransformingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects.Path
import ch.datascience.microservices.MicroserviceBaseUrl
import doobie.implicits._

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
    maybeMessage match {
      case None =>
        sql"""|INSERT INTO
              |event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
              |VALUES (${compoundEventId.id}, ${compoundEventId.projectId}, $eventStatus, $createdDate, $executionDate, $eventDate, $batchDate, $eventBody)
      """.stripMargin.update.run.void
      case Some(message) =>
        sql"""|INSERT INTO
              |event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
              |VALUES (${compoundEventId.id}, ${compoundEventId.projectId}, $eventStatus, $createdDate, $executionDate, $eventDate, $batchDate, $eventBody, $message)
      """.stripMargin.update.run.void
    }
  }

  protected def upsertProject(compoundEventId: CompoundEventId, projectPath: Path, eventDate: EventDate): Unit =
    execute {
      sql"""|INSERT INTO
            |project (project_id, project_path, latest_event_date)
            |VALUES (${compoundEventId.projectId}, $projectPath, $eventDate)
            |ON CONFLICT (project_id)
            |DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
      """.stripMargin.update.run.void
    }

  protected def upsertEventPayload(compoundEventId: CompoundEventId,
                                   eventStatus:     EventStatus,
                                   schemaVersion:   SchemaVersion,
                                   maybePayload:    Option[EventPayload]
  ): Unit = (eventStatus, maybePayload) match {
    case (TriplesGenerated | TransformationRecoverableFailure | TransformingTriples, Some(payload)) =>
      execute {
        sql"""|INSERT INTO
              |event_payload (event_id, project_id, payload, schema_version)
              |VALUES (${compoundEventId.id}, ${compoundEventId.projectId}, ${payload.value}, ${schemaVersion.value})
              |ON CONFLICT (event_id, project_id, schema_version)
              |DO UPDATE SET payload = excluded.payload
      """.stripMargin.update.run.void
      }
    case _ => ()
  }

  protected def upsertProcessingTime(compoundEventId: CompoundEventId,
                                     eventStatus:     EventStatus,
                                     processingTime:  EventProcessingTime
  ): Unit = execute {
    sql"""|INSERT INTO
          |status_processing_time (event_id, project_id, status, processing_time)
          |VALUES (${compoundEventId.id}, ${compoundEventId.projectId}, $eventStatus, $processingTime)
          |ON CONFLICT (event_id, project_id, status)
          |DO UPDATE SET processing_time = excluded.processing_time
      """.stripMargin.update.run.void
  }

  protected def upsertEventDelivery(compoundEventId: CompoundEventId, deliveryId: SubscriberId): Unit = execute {
    sql"""|INSERT INTO
          |event_delivery (event_id, project_id, delivery_id)
          |VALUES (${compoundEventId.id}, ${compoundEventId.projectId}, $deliveryId)
          |ON CONFLICT (event_id, project_id, delivery_id)
          |DO NOTHING
      """.stripMargin.update.run.void
  }

  protected def upsertSubscriber(deliveryId:  SubscriberId,
                                 deliveryUrl: SubscriberUrl,
                                 sourceUrl:   MicroserviceBaseUrl
  ): Unit = execute {
    sql"""|INSERT INTO
          |subscriber (delivery_id, delivery_url, source_url)
          |VALUES ($deliveryId, $deliveryUrl, $sourceUrl)
          |ON CONFLICT (delivery_url, source_url)
          |DO NOTHING
      """.stripMargin.update.run.void
  }

}
