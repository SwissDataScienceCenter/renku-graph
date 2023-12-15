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
import io.circe.Json
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.events.Generators.{subscriberIds, subscriberUrls}
import io.renku.events.Subscription.{SubscriberId, SubscriberUrl}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventContentGenerators.{eventDates, eventMessages}
import io.renku.graph.model.EventsGenerators.{compoundEventIds, eventBodies, eventProcessingTimes, zippedEventPayloads}
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, TransformationNonRecoverableFailure, TransformationRecoverableFailure, TransformingTriples, TriplesGenerated, TriplesStore}
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus, _}
import io.renku.graph.model.projects
import io.renku.microservices.MicroserviceBaseUrl
import skunk._
import skunk.codec.all.{text, timestamptz, varchar}
import skunk.implicits._

import java.time.{Instant, OffsetDateTime}
import scala.util.Random

trait EventLogDBProvisioning {
  self: EventLogPostgresSpec with TypeSerializers =>

  protected case class GeneratedEvent(eventId:         CompoundEventId,
                                      status:          EventStatus,
                                      eventDate:       EventDate,
                                      project:         Project,
                                      maybeMessage:    Option[EventMessage],
                                      maybePayload:    Option[ZippedEventPayload],
                                      processingTimes: List[EventProcessingTime]
  )
  protected def storeGeneratedEvent(status:        EventStatus,
                                    eventDate:     EventDate = timestampsNotInTheFuture.generateAs(EventDate),
                                    project:       Project = consumerProjects.generateOne,
                                    executionDate: ExecutionDate = timestampsNotInTheFuture.generateAs(ExecutionDate),
                                    message:       Option[EventMessage] = None
  )(implicit cfg: DBConfig[EventLogDB]): IO[GeneratedEvent] = {
    val eventId = compoundEventIds(project.id).generateOne
    val maybeMessage = status match {
      case _: EventStatus.FailureStatus => message orElse eventMessages.generateSome
      case _ => message orElse eventMessages.generateOption
    }
    val maybePayload = status match {
      case TriplesGenerated | TransformingTriples | TriplesStore => zippedEventPayloads.generateSome
      case AwaitingDeletion                                      => zippedEventPayloads.generateOption
      case _                                                     => zippedEventPayloads.generateNone
    }

    for {
      _ <- storeEvent(eventId,
                      status,
                      executionDate,
                      eventDate,
                      eventBodies.generateOne,
                      projectSlug = project.slug,
                      maybeMessage = maybeMessage,
                      maybeEventPayload = maybePayload
           )
      processingTimes = status match {
                          case TriplesGenerated | TriplesStore => List(eventProcessingTimes.generateOne)
                          case AwaitingDeletion =>
                            if (Random.nextBoolean()) List(eventProcessingTimes.generateOne)
                            else Nil
                          case _ => Nil
                        }
      _ <- processingTimes.traverse_(upsertProcessingTime(eventId, status, _))
    } yield GeneratedEvent(eventId, status, eventDate, project, maybeMessage, maybePayload, processingTimes)
  }

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
    upsertProject(Project(compoundEventId.projectId, projectSlug), eventDate) >>
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

  protected def upsertProject(project: Project, eventDate: EventDate = eventDates.generateOne)(implicit
      cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[projects.GitLabId *: projects.Slug *: EventDate *: EmptyTuple] = sql"""
          INSERT INTO project (project_id, project_slug, latest_event_date)
          VALUES ($projectIdEncoder, $projectSlugEncoder, $eventDateEncoder)
          ON CONFLICT (project_id)
          DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
          """.command
      session.prepare(query).flatMap(_.execute(project.id *: project.slug *: eventDate *: EmptyTuple)).void
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

  protected def upsertProcessingTime(compoundEventId: CompoundEventId,
                                     eventStatus:     EventStatus,
                                     processingTime:  EventProcessingTime
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[EventId *: projects.GitLabId *: EventStatus *: EventProcessingTime *: EmptyTuple] =
        sql"""INSERT INTO status_processing_time (event_id, project_id, status, processing_time)
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

  protected def upsertEventDeliveryInfo(
      eventId:     CompoundEventId,
      deliveryId:  SubscriberId = subscriberIds.generateOne,
      deliveryUrl: SubscriberUrl = subscriberUrls.generateOne,
      sourceUrl:   MicroserviceBaseUrl = microserviceBaseUrls.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    upsertSubscriber(deliveryId, deliveryUrl, sourceUrl) >>
      upsertEventDelivery(eventId, deliveryId)

  protected def upsertEventDelivery(eventId: CompoundEventId, deliveryId: SubscriberId = subscriberIds.generateOne)(
      implicit cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[EventId *: projects.GitLabId *: SubscriberId *: EmptyTuple] =
        sql"""INSERT INTO event_delivery (event_id, project_id, delivery_id)
              VALUES ($eventIdEncoder, $projectIdEncoder, $subscriberIdEncoder)
              ON CONFLICT (event_id, project_id)
              DO NOTHING
        """.command
      session.prepare(query).flatMap(_.execute(eventId.id *: eventId.projectId *: deliveryId *: EmptyTuple)).void
    }

  protected def upsertSubscriber(deliveryId: SubscriberId, deliveryUrl: SubscriberUrl, sourceUrl: MicroserviceBaseUrl)(
      implicit cfg: DBConfig[EventLogDB]
  ): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[SubscriberId *: SubscriberUrl *: MicroserviceBaseUrl *: SubscriberId *: EmptyTuple] =
        sql"""INSERT INTO subscriber (delivery_id, delivery_url, source_url)
              VALUES ($subscriberIdEncoder, $subscriberUrlEncoder, $microserviceBaseUrlEncoder)
              ON CONFLICT (delivery_url, source_url)
              DO UPDATE SET delivery_id = $subscriberIdEncoder, delivery_url = EXCLUDED.delivery_url, source_url = EXCLUDED.source_url
        """.command

      session
        .prepare(query)
        .flatMap(_.execute(deliveryId *: deliveryUrl *: sourceUrl *: deliveryId *: EmptyTuple))
        .void
    }

  def insertEventIntoEventsQueue(eventType: String, payload: Json)(implicit cfg: DBConfig[EventLogDB]): IO[Unit] =
    moduleSessionResource(cfg).session.use { session =>
      val query: Command[OffsetDateTime *: String *: String *: EmptyTuple] =
        sql"""INSERT INTO status_change_events_queue (date, event_type, payload)
              VALUES ($timestamptz, $varchar, $text)""".command
      session
        .prepare(query)
        .flatMap(_.execute(OffsetDateTime.now() *: eventType *: payload.noSpaces *: EmptyTuple))
        .void
    }
}
