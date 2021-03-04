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

import cats.MonadError
import cats.effect.{Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.{events, projects}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}

private[eventlog] trait EventDelivery[Interpretation[_], CategoryEvent] {
  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit]
  def unregister(event:      CompoundEventId): Interpretation[Unit]
}

private class EventDeliveryImpl[CategoryEvent](transactor:               DbTransactor[IO, EventLogDB],
                                               compoundEventIdExtractor: CategoryEvent => CompoundEventId,
                                               queriesExecTimes:         LabeledHistogram[IO, SqlQuery.Name],
                                               sourceUrl:                MicroserviceBaseUrl
)(implicit ME:                                                           Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventDelivery[IO, CategoryEvent]
    with TypeSerializers {

  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): IO[Unit] = {
    val CompoundEventId(id, projectId) = compoundEventIdExtractor(event)
    for {
      _      <- deleteDelivery(id, projectId)
      result <- insert(id, projectId, subscriberUrl)
    } yield result
  } transact transactor.get flatMap toResult

  def unregister(eventId: CompoundEventId): IO[Unit] =
    deleteDelivery(eventId.id, eventId.projectId) transact transactor.get flatMap toResult

  private def deleteDelivery(eventId: events.EventId, projectId: projects.Id) = measureExecutionTime {
    SqlQuery(
      sql"""|DELETE FROM event_delivery 
            |WHERE event_id = $eventId AND project_id = $projectId
            |""".stripMargin.update.run,
      name = "event delivery info - remove"
    )
  }

  private def insert(eventId: events.EventId, projectId: projects.Id, subscriberUrl: SubscriberUrl) =
    measureExecutionTime {
      SqlQuery(
        sql"""|INSERT INTO event_delivery (event_id, project_id, delivery_id)
              |  SELECT $eventId, $projectId, delivery_id
              |  FROM subscriber
              |  WHERE delivery_url = $subscriberUrl AND source_url = $sourceUrl
              |ON CONFLICT (event_id, project_id)
              |DO NOTHING
              |""".stripMargin.update.run,
        name = "event delivery info - insert"
      )
    }

  private lazy val toResult: Int => IO[Unit] = {
    case 0 | 1 => ().pure[IO]
    case _     => new Exception("Inserted more than one record to the event_delivery").raiseError[IO, Unit]
  }
}

private[eventlog] object EventDelivery {

  def apply[CategoryEvent](
      transactor:               DbTransactor[IO, EventLogDB],
      compoundEventIdExtractor: CategoryEvent => CompoundEventId,
      queriesExecTimes:         LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventDelivery[IO, CategoryEvent]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    microserviceUrl       <- microserviceUrlFinder.findBaseUrl()
  } yield new EventDeliveryImpl[CategoryEvent](transactor, compoundEventIdExtractor, queriesExecTimes, microserviceUrl)

  def noOp[Interpretation[_]: MonadError[*[_], Throwable], CategoryEvent]
      : Interpretation[EventDelivery[Interpretation, CategoryEvent]] =
    new NoOpEventDelivery[Interpretation, CategoryEvent]()
      .pure[Interpretation]
      .widen[EventDelivery[Interpretation, CategoryEvent]]
}

private class NoOpEventDelivery[Interpretation[_]: MonadError[*[_], Throwable], CategoryEvent]
    extends EventDelivery[Interpretation, CategoryEvent] {

  override def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit] =
    ().pure[Interpretation]

  override def unregister(eventId: CompoundEventId): Interpretation[Unit] =
    ().pure[Interpretation]
}
