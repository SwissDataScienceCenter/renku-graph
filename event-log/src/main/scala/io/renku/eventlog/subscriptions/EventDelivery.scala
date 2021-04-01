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
import cats.data.Kleisli
import cats.effect.{Async, Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.{CompoundEventId, EventId}
import ch.datascience.graph.model.{events, projects}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, Microservice, TypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

private[subscriptions] trait EventDelivery[Interpretation[_], CategoryEvent] {
  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit]
  def unregister(event:      CompoundEventId): Interpretation[Unit]
}

private class EventDeliveryImpl[Interpretation[_]: Async: Bracket[*[_], Throwable], CategoryEvent](
    transactor:               SessionResource[Interpretation, EventLogDB],
    compoundEventIdExtractor: CategoryEvent => CompoundEventId,
    queriesExecTimes:         LabeledHistogram[Interpretation, SqlQuery.Name],
    sourceUrl:                MicroserviceBaseUrl
) extends DbClient(Some(queriesExecTimes))
    with EventDelivery[Interpretation, CategoryEvent]
    with TypeSerializers {

  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit] = transactor.use {
    implicit session =>
      val CompoundEventId(id, projectId) = compoundEventIdExtractor(event)
      for {
        _      <- deleteDelivery(id, projectId)
        result <- insert(id, projectId, subscriberUrl)
      } yield result
  } flatMap toResult

  // TODO not sure if a transaction is needed

  def unregister(eventId: CompoundEventId): Interpretation[Unit] = transactor.use { implicit session =>
    deleteDelivery(eventId.id, eventId.projectId)
  }

  private def insert(eventId: events.EventId, projectId: projects.Id, subscriberUrl: SubscriberUrl)(implicit
      session:                Session[Interpretation]
  ) =
    measureExecutionTime {
      SqlQuery(
        Kleisli { session =>
          val query: Command[EventId ~ projects.Id ~ SubscriberUrl ~ MicroserviceBaseUrl] = sql"""
           INSERT INTO event_delivery (event_id, project_id, delivery_id)
             SELECT $eventIdPut, $projectIdPut, delivery_id
             FROM subscriber
             WHERE delivery_url = $subscriberUrlPut AND source_url = $microserviceBaseUrlPut
           ON CONFLICT (event_id, project_id)
           DO NOTHING
           """.command
          session.prepare(query).use(_.execute(eventId ~ projectId ~ subscriberUrl ~ sourceUrl))
        },
        name = "event delivery info - insert"
      )
    }

  private def deleteDelivery(eventId: events.EventId, projectId: projects.Id)(implicit
      session:                        Session[Interpretation]
  ) = measureExecutionTime {
    SqlQuery(
      Kleisli { session =>
        val query: Command[EventId ~ projects.Id] = sql"""DELETE FROM event_delivery
                                                        WHERE event_id = $eventIdPut AND project_id = $projectIdPut
                                                     """.command
        session.prepare(query).use(_.execute(eventId ~ projectId)).void
      },
      name = "event delivery info - remove"
    )
  }

  private lazy val toResult: Completion => Interpretation[Unit] = {
    case Completion.Insert(0 | 1) => ().pure[Interpretation]
    case _                        => new Exception("Inserted more than one record to the event_delivery").raiseError[Interpretation, Unit]
  }
}

private[subscriptions] object EventDelivery {

  def apply[CategoryEvent](
      transactor:               SessionResource[IO, EventLogDB],
      compoundEventIdExtractor: CategoryEvent => CompoundEventId,
      queriesExecTimes:         LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventDelivery[IO, CategoryEvent]] = for {
    microserviceUrlFinder <- MicroserviceUrlFinder(Microservice.ServicePort)
    microserviceUrl       <- microserviceUrlFinder.findBaseUrl()
  } yield new EventDeliveryImpl[IO, CategoryEvent](transactor,
                                                   compoundEventIdExtractor,
                                                   queriesExecTimes,
                                                   microserviceUrl
  )

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
