/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions

import cats.MonadThrow
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.graph.model.events.{CompoundEventId, DeletingProjectDeliverId, EventDeliveryId, EventId, EventTypeId}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private[subscriptions] trait EventDelivery[F[_], CategoryEvent] {
  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): F[Unit]
}

private class EventDeliveryImpl[F[_]: MonadCancelThrow, CategoryEvent](
    sessionResource:          SessionResource[F, EventLogDB],
    compoundEventIdExtractor: CategoryEvent => EventDeliveryId,
    queriesExecTimes:         LabeledHistogram[F, SqlStatement.Name],
    sourceUrl:                MicroserviceBaseUrl
) extends DbClient(Some(queriesExecTimes))
    with EventDelivery[F, CategoryEvent]
    with TypeSerializers {

  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): F[Unit] = sessionResource.useK {
    val eventDeliveryId = compoundEventIdExtractor(event)
    for {
      _      <- deleteDelivery(eventDeliveryId)
      result <- insert(eventDeliveryId, subscriberUrl)
    } yield result
  } flatMap toResult

  private def insert(eventDeliveryId: EventDeliveryId, subscriberUrl: SubscriberUrl) =
    measureExecutionTime {
      eventDeliveryId match {
        case CompoundEventId(eventId, projectId) =>
          SqlStatement(name = "event delivery info - insert")
            .command[EventId ~ projects.Id ~ SubscriberUrl ~ MicroserviceBaseUrl](
              sql"""INSERT INTO event_delivery (event_id, project_id, delivery_id)
                SELECT $eventIdEncoder, $projectIdEncoder, delivery_id
                FROM subscriber
                WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder
                ON CONFLICT (event_id, project_id)
                DO NOTHING
            """.command
            )
            .arguments(eventId ~ projectId ~ subscriberUrl ~ sourceUrl)
            .build
        case eventId @ DeletingProjectDeliverId(projectId) =>
          SqlStatement(name = "event delivery info - insert")
            .command[projects.Id ~ EventTypeId ~ SubscriberUrl ~ MicroserviceBaseUrl](
              sql"""INSERT INTO event_delivery (project_id, delivery_id, event_type_id)
                SELECT  $projectIdEncoder, delivery_id, $eventTypeIdEncoder
                FROM subscriber
                WHERE delivery_url = $subscriberUrlEncoder AND source_url = $microserviceBaseUrlEncoder
                ON CONFLICT (event_id, project_id)
                DO NOTHING
            """.command
            )
            .arguments(projectId ~ eventId.eventTypeId ~ subscriberUrl ~ sourceUrl)
            .build
      }
    }

  private def deleteDelivery(eventDeliveryId: EventDeliveryId) = measureExecutionTime {
    eventDeliveryId match {
      case CompoundEventId(eventId, projectId) =>
        SqlStatement(name = "event delivery info - remove")
          .command[EventId ~ projects.Id](
            sql"""DELETE FROM event_delivery
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
          """.command
          )
          .arguments(eventId ~ projectId)
          .build
          .void
      case eventId @ DeletingProjectDeliverId(projectId) =>
        SqlStatement(name = "event delivery info - remove")
          .command[projects.Id ~ EventTypeId](
            sql"""DELETE FROM event_delivery
              WHERE event_id = NULL AND project_id = $projectIdEncoder AND event_type_id = $eventTypeIdEncoder
          """.command
          )
          .arguments(projectId ~ eventId.eventTypeId)
          .build
          .void
    }
  }

  private lazy val toResult: Completion => F[Unit] = {
    case Completion.Insert(0 | 1) => ().pure[F]
    case _ => new Exception("Inserted more than one record to the event_delivery").raiseError[F, Unit]
  }
}

private[subscriptions] object EventDelivery {

  def apply[F[_]: MonadCancelThrow, CategoryEvent](
      sessionResource:          SessionResource[F, EventLogDB],
      compoundEventIdExtractor: CategoryEvent => EventDeliveryId,
      queriesExecTimes:         LabeledHistogram[F, SqlStatement.Name]
  ): F[EventDelivery[F, CategoryEvent]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    microserviceUrl       <- microserviceUrlFinder.findBaseUrl()
  } yield new EventDeliveryImpl[F, CategoryEvent](sessionResource,
                                                  compoundEventIdExtractor,
                                                  queriesExecTimes,
                                                  microserviceUrl
  )

  def noOp[F[_]: MonadThrow, CategoryEvent]: F[EventDelivery[F, CategoryEvent]] =
    new NoOpEventDelivery[F, CategoryEvent]()
      .pure[F]
      .widen[EventDelivery[F, CategoryEvent]]
}

private class NoOpEventDelivery[F[_]: MonadThrow, CategoryEvent] extends EventDelivery[F, CategoryEvent] {
  override def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): F[Unit] = ().pure[F]
}
