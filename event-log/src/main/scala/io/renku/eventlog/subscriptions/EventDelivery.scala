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
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, TypeSerializers}

private trait EventDelivery[Interpretation[_], CategoryEvent] {
  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): Interpretation[Unit]
}

private class EventDeliveryImpl[CategoryEvent](transactor:               DbTransactor[IO, EventLogDB],
                                               compoundEventIdExtractor: CategoryEvent => CompoundEventId,
                                               queriesExecTimes:         LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:                                                           Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventDelivery[IO, CategoryEvent]
    with TypeSerializers {

  def registerSending(event: CategoryEvent, subscriberUrl: SubscriberUrl): IO[Unit] = measureExecutionTime {
    val CompoundEventId(id, projectId) = compoundEventIdExtractor(event)

    SqlQuery(
      sql"""|INSERT INTO event_delivery (event_id, project_id, delivery_url)
            |VALUES ($id, $projectId, $subscriberUrl)
            |ON CONFLICT (event_id, project_id, delivery_url)
            |DO NOTHING
            |""".stripMargin.update.run,
      name = "event delivery info - add"
    )
  } transact transactor.get flatMap toResult

  private lazy val toResult: Int => IO[Unit] = {
    case 0 | 1 => ().pure[IO]
    case _     => new Exception("Inserted more than one record to the event_delivery").raiseError[IO, Unit]
  }
}

private object EventDelivery {

  def apply[CategoryEvent](
      transactor:               DbTransactor[IO, EventLogDB],
      compoundEventIdExtractor: CategoryEvent => CompoundEventId,
      queriesExecTimes:         LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventDelivery[IO, CategoryEvent]] = IO {
    new EventDeliveryImpl[CategoryEvent](transactor, compoundEventIdExtractor, queriesExecTimes)
  }

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
}
