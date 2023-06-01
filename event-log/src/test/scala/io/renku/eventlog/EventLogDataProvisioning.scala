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

import cats.data.Kleisli
import io.circe.Json
import io.renku.eventlog.events.producers.eventdelivery._
import io.renku.events.Generators.{subscriberIds, subscriberUrls}
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}
import io.renku.events.consumers
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventContentGenerators.eventMessages
import io.renku.graph.model.EventsGenerators.{eventBodies, eventIds, eventProcessingTimes, zippedEventPayloads}
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, TransformationRecoverableFailure, TransformingTriples, TriplesGenerated, TriplesStore}
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Path
import io.renku.microservices.MicroserviceBaseUrl
import skunk._
import skunk.codec.all.{text, timestamptz, varchar}
import skunk.implicits._

import java.time.{Instant, OffsetDateTime}
import scala.util.Random

trait EventLogDataProvisioning {
  self: InMemoryEventLogDb =>

  protected def storeGeneratedEvent(status:      EventStatus,
                                    eventDate:   EventDate,
                                    projectId:   projects.GitLabId,
                                    projectPath: projects.Path,
                                    message:     Option[EventMessage] = None
  ): (EventId, EventStatus, Option[EventMessage], Option[ZippedEventPayload], List[EventProcessingTime]) = {
    val eventId = CompoundEventId(eventIds.generateOne, projectId)
    val maybeMessage = status match {
      case _: EventStatus.FailureStatus => message orElse eventMessages.generateSome
      case _ => message orElse eventMessages.generateOption
    }
    val maybePayload = status match {
      case TriplesGenerated | TransformingTriples | TriplesStore => zippedEventPayloads.generateSome
      case AwaitingDeletion                                      => zippedEventPayloads.generateOption
      case _                                                     => zippedEventPayloads.generateNone
    }
    storeEvent(
      eventId,
      status,
      timestampsNotInTheFuture.generateAs(ExecutionDate),
      eventDate,
      eventBodies.generateOne,
      projectPath = projectPath,
      maybeMessage = maybeMessage,
      maybeEventPayload = maybePayload
    )

    val processingTimes = status match {
      case TriplesGenerated | TriplesStore => List(eventProcessingTimes.generateOne)
      case AwaitingDeletion =>
        if (Random.nextBoolean()) List(eventProcessingTimes.generateOne)
        else Nil
      case _ => Nil
    }
    processingTimes.foreach(upsertProcessingTime(eventId, status, _))

    (eventId.id, status, maybeMessage, maybePayload, processingTimes)
  }

  protected def storeEvent(compoundEventId:   CompoundEventId,
                           eventStatus:       EventStatus,
                           executionDate:     ExecutionDate,
                           eventDate:         EventDate,
                           eventBody:         EventBody,
                           createdDate:       CreatedDate = CreatedDate(Instant.now),
                           batchDate:         BatchDate = BatchDate(Instant.now),
                           projectPath:       Path = projectPaths.generateOne,
                           maybeMessage:      Option[EventMessage] = None,
                           maybeEventPayload: Option[ZippedEventPayload] = None
  ): Unit = {
    upsertProject(compoundEventId, projectPath, eventDate)
    insertEvent(compoundEventId, eventStatus, executionDate, eventDate, eventBody, createdDate, batchDate, maybeMessage)
    upsertEventPayload(compoundEventId, eventStatus, maybeEventPayload)
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
            EventId *: projects.GitLabId *: EventStatus *: CreatedDate *: ExecutionDate *: EventDate *: BatchDate *: EventBody *: EmptyTuple
          ] =
            sql"""INSERT INTO
                  event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
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
          ] =
            sql"""INSERT INTO
                  event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
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
  }

  protected def upsertProject(compoundEventId: CompoundEventId, projectPath: Path, eventDate: EventDate): Unit =
    upsertProject(compoundEventId.projectId, projectPath, eventDate)

  protected def upsertProject(project: consumers.Project, eventDate: EventDate): Unit =
    upsertProject(project.id, project.path, eventDate)

  protected def upsertProject(projectId: projects.GitLabId, projectPath: Path, eventDate: EventDate): Unit = execute {
    Kleisli { session =>
      val query: Command[projects.GitLabId *: projects.Path *: EventDate *: EmptyTuple] =
        sql"""INSERT INTO project (project_id, project_path, latest_event_date)
              VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
              ON CONFLICT (project_id)
              DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
          """.command
      session.prepare(query).flatMap(_.execute(projectId *: projectPath *: eventDate *: EmptyTuple)).void
    }
  }

  protected def upsertEventPayload(compoundEventId: CompoundEventId,
                                   eventStatus:     EventStatus,
                                   maybePayload:    Option[ZippedEventPayload]
  ): Unit = eventStatus match {
    case TriplesGenerated | TransformationRecoverableFailure | TransformingTriples | TriplesStore | AwaitingDeletion =>
      maybePayload
        .map { payload =>
          execute[Unit] {
            Kleisli { session =>
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
        }
        .getOrElse(())
    case _ => ()
  }

  protected def upsertProcessingTime(compoundEventId: CompoundEventId,
                                     eventStatus:     EventStatus,
                                     processingTime:  EventProcessingTime
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[EventId *: projects.GitLabId *: EventStatus *: EventProcessingTime *: EmptyTuple] =
        sql"""INSERT INTO
              status_processing_time (event_id, project_id, status, processing_time)
              VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $eventProcessingTimeEncoder)
              ON CONFLICT (event_id, project_id, status)
              DO UPDATE SET processing_time = excluded.processing_time
        """.command
      session
        .prepare(query)
        .flatMap(
          _.execute(compoundEventId.id *: compoundEventId.projectId *: eventStatus *: processingTime *: EmptyTuple)
        )
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
      val query: Command[SubscriberId *: SubscriberUrl *: MicroserviceBaseUrl *: SubscriberId *: EmptyTuple] =
        sql"""INSERT INTO
              subscriber (delivery_id, delivery_url, source_url)
              VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
              ON CONFLICT (delivery_url, source_url)
              DO UPDATE SET delivery_id = $subscriberIdEncoder, delivery_url = EXCLUDED.delivery_url, source_url = EXCLUDED.source_url
        """.command

      session.prepare(query).flatMap(_.execute(deliveryId *: deliveryUrl *: sourceUrl *: deliveryId *: EmptyTuple)).void
    }
  }

  protected def upsertEventDelivery(eventId:    CompoundEventId,
                                    deliveryId: SubscriberId = subscriberIds.generateOne
  ): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[EventId *: projects.GitLabId *: SubscriberId *: EmptyTuple] =
        sql"""INSERT INTO event_delivery (event_id, project_id, delivery_id)
              VALUES ($eventIdEncoder, $projectIdEncoder, $subscriberIdEncoder)
              ON CONFLICT (event_id, project_id)
              DO NOTHING
        """.command
      session.prepare(query).flatMap(_.execute(eventId.id *: eventId.projectId *: deliveryId *: EmptyTuple)).void
    }
  }

  protected def findAllEventDeliveries: List[(CompoundEventId, SubscriberId)] = execute {
    Kleisli { session =>
      val query: Query[Void, (CompoundEventId, SubscriberId)] =
        sql"""SELECT event_id, project_id, delivery_id
              FROM event_delivery WHERE event_id IS NOT NULL"""
          .query(eventIdDecoder ~ projectIdDecoder ~ subscriberIdDecoder)
          .map { case eventId ~ projectId ~ subscriberId =>
            (CompoundEventId(eventId, projectId), subscriberId)
          }

      session.execute(query)
    }
  }

  protected def findAllProjectDeliveries: List[(projects.GitLabId, SubscriberId, EventTypeId)] = execute {
    Kleisli { session =>
      val query: Query[Void, (projects.GitLabId, SubscriberId, EventTypeId)] =
        sql"""SELECT project_id, delivery_id, event_type_id
              FROM event_delivery
              WHERE event_id IS NULL"""
          .query(projectIdDecoder ~ subscriberIdDecoder ~ eventTypeIdDecoder)
          .map { case projectId ~ subscriberId ~ eventTypeId =>
            (projectId, subscriberId, eventTypeId)
          }

      session.execute(query)
    }
  }

  def insertEventIntoEventsQueue(eventType: String, payload: Json): Unit = execute {
    Kleisli { session =>
      val query: Command[OffsetDateTime *: String *: String *: EmptyTuple] =
        sql"""INSERT INTO status_change_events_queue (date, event_type, payload)
              VALUES ($timestamptz, $varchar, $text)""".command
      session
        .prepare(query)
        .flatMap(_.execute(OffsetDateTime.now() *: eventType *: payload.noSpaces *: EmptyTuple))
        .void
    }
  }
}
