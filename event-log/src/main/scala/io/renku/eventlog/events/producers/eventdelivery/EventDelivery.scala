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

package io.renku.eventlog.events.producers
package eventdelivery

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{Microservice, TypeSerializers}
import io.renku.events.Subscription.SubscriberUrl
import io.renku.graph.model.events.{CompoundEventId, EventId}
import io.renku.graph.model.projects
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private[producers] trait EventDelivery[F[_], CategoryEvent] {
  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): F[Unit]
}

private class EventDeliveryImpl[F[_]: Async: SessionResource: QueriesExecutionTimes, CategoryEvent](
    eventDeliveryIdExtractor: CategoryEvent => EventDeliveryId,
    sourceUrl:                MicroserviceBaseUrl
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with EventDelivery[F, CategoryEvent]
    with TypeSerializers {

  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): F[Unit] = SessionResource[F].useK {
    val eventDeliveryId = eventDeliveryIdExtractor(event)
    deleteDelivery(eventDeliveryId) >> insert(eventDeliveryId, subscriberUrl)
  } flatMap toResult

  private def insert(eventDeliveryId: EventDeliveryId, subscriberUrl: SubscriberUrl) = measureExecutionTime {
    eventDeliveryId match {
      case CompoundEventDeliveryId(CompoundEventId(eventId, projectId)) =>
        SqlStatement(name = "event delivery info - insert")
          .command[EventId *: projects.GitLabId *: SubscriberUrl *: MicroserviceBaseUrl *: EmptyTuple](sql"""
              INSERT INTO event_delivery (event_id, project_id, delivery_id)
              SELECT $eventIdEncoder, $projectIdEncoder, delivery_id
              FROM subscriber
              WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder
              ON CONFLICT (event_id, project_id)
              DO NOTHING
              """.command)
          .arguments(eventId *: projectId *: subscriberUrl *: sourceUrl *: EmptyTuple)
          .build
      case eventId @ DeletingProjectDeliverId(projectId) =>
        SqlStatement(name = "event delivery info - insert")
          .command[projects.GitLabId *: EventTypeId *: SubscriberUrl *: MicroserviceBaseUrl *: EmptyTuple](sql"""
              INSERT INTO event_delivery (project_id, delivery_id, event_type_id)
              SELECT  $projectIdEncoder, delivery_id, $eventTypeIdEncoder
              FROM subscriber
              WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder
              ON CONFLICT (project_id, event_type_id)
              DO NOTHING
              """.command)
          .arguments(projectId *: eventId.eventTypeId *: subscriberUrl *: sourceUrl *: EmptyTuple)
          .build
    }
  }

  private def deleteDelivery(eventDeliveryId: EventDeliveryId) = measureExecutionTime {
    eventDeliveryId match {
      case CompoundEventDeliveryId(CompoundEventId(eventId, projectId)) =>
        SqlStatement(name = "event delivery info - remove")
          .command[EventId *: projects.GitLabId *: EmptyTuple](sql"""
            DELETE FROM event_delivery
            WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.command)
          .arguments(eventId *: projectId *: EmptyTuple)
          .build
          .void
      case eventId @ DeletingProjectDeliverId(projectId) =>
        SqlStatement(name = "event delivery info - remove")
          .command[projects.GitLabId *: EventTypeId *: EmptyTuple](sql"""
            DELETE FROM event_delivery
            WHERE event_id = NULL AND project_id = $projectIdEncoder AND event_type_id = $eventTypeIdEncoder
            """.command)
          .arguments(projectId *: eventId.eventTypeId *: EmptyTuple)
          .build
          .void
    }
  }

  private lazy val toResult: Completion => F[Unit] = {
    case Completion.Insert(0 | 1) => ().pure[F]
    case _ => new Exception("Inserted more than one record to the event_delivery").raiseError[F, Unit]
  }
}

private[producers] object EventDelivery {

  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes, CategoryEvent](
      eventDeliveryIdExtractor: CategoryEvent => EventDeliveryId
  ): F[EventDelivery[F, CategoryEvent]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    microserviceUrl       <- microserviceUrlFinder.findBaseUrl()
  } yield new EventDeliveryImpl[F, CategoryEvent](eventDeliveryIdExtractor, microserviceUrl)

  def noOp[F[_]: MonadThrow, CategoryEvent]: F[EventDelivery[F, CategoryEvent]] =
    new NoOpEventDelivery[F, CategoryEvent]()
      .pure[F]
      .widen[EventDelivery[F, CategoryEvent]]
}

private[producers] class NoOpEventDelivery[F[_]: MonadThrow, CategoryEvent] extends EventDelivery[F, CategoryEvent] {
  override def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): F[Unit] = ().pure[F]
}
