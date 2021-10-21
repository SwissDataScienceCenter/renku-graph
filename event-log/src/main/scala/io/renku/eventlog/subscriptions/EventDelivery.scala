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

package io.renku.eventlog.subscriptions

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import cats.{MonadError, MonadThrow}
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}
import io.renku.events.consumers.subscriptions.SubscriberUrl
import io.renku.graph.model.events.{CompoundEventId, EventId}
import io.renku.graph.model.{events, projects}
import io.renku.metrics.LabeledHistogram
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private[subscriptions] trait EventDelivery[Interpretation[_], CategoryEvent] {
  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit]
}

private class EventDeliveryImpl[Interpretation[_]: MonadCancelThrow, CategoryEvent](
    sessionResource:          SessionResource[Interpretation, EventLogDB],
    compoundEventIdExtractor: CategoryEvent => CompoundEventId,
    queriesExecTimes:         LabeledHistogram[Interpretation, SqlStatement.Name],
    sourceUrl:                MicroserviceBaseUrl
) extends DbClient(Some(queriesExecTimes))
    with EventDelivery[Interpretation, CategoryEvent]
    with TypeSerializers {

  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit] = sessionResource.useK {
    val CompoundEventId(id, projectId) = compoundEventIdExtractor(event)
    for {
      _      <- deleteDelivery(id, projectId)
      result <- insert(id, projectId, subscriberUrl)
    } yield result
  } flatMap toResult

  private def insert(eventId: events.EventId, projectId: projects.Id, subscriberUrl: SubscriberUrl) =
    measureExecutionTime {
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
    }

  private def deleteDelivery(eventId: events.EventId, projectId: projects.Id) = measureExecutionTime {
    SqlStatement(name = "event delivery info - remove")
      .command[EventId ~ projects.Id](
        sql"""DELETE FROM event_delivery
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
          """.command
      )
      .arguments(eventId ~ projectId)
      .build
      .void
  }

  private lazy val toResult: Completion => Interpretation[Unit] = {
    case Completion.Insert(0 | 1) => ().pure[Interpretation]
    case _ => new Exception("Inserted more than one record to the event_delivery").raiseError[Interpretation, Unit]
  }
}

private[subscriptions] object EventDelivery {

  def apply[F[_]: MonadCancelThrow, CategoryEvent](
      sessionResource:          SessionResource[F, EventLogDB],
      compoundEventIdExtractor: CategoryEvent => CompoundEventId,
      queriesExecTimes:         LabeledHistogram[F, SqlStatement.Name]
  ): F[EventDelivery[F, CategoryEvent]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    microserviceUrl       <- microserviceUrlFinder.findBaseUrl()
  } yield new EventDeliveryImpl[F, CategoryEvent](sessionResource,
                                                  compoundEventIdExtractor,
                                                  queriesExecTimes,
                                                  microserviceUrl
  )

  def noOp[Interpretation[_]: MonadThrow, CategoryEvent]: Interpretation[EventDelivery[Interpretation, CategoryEvent]] =
    new NoOpEventDelivery[Interpretation, CategoryEvent]()
      .pure[Interpretation]
      .widen[EventDelivery[Interpretation, CategoryEvent]]
}

private class NoOpEventDelivery[Interpretation[_]: MonadError[*[_], Throwable], CategoryEvent]
    extends EventDelivery[Interpretation, CategoryEvent] {

  override def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit] =
    ().pure[Interpretation]
}
