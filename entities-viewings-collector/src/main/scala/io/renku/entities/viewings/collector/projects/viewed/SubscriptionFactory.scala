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

package io.renku.entities.viewings.collector.projects.viewed

import cats.effect.Async
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.{CategoryName, consumers}
import io.renku.eventsqueue.EventsQueue
import io.renku.lock.Lock
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

object SubscriptionFactory {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder, DB](
      tsReadyCheck:    F[Boolean],
      categoryLock:    Lock[F, CategoryName],
      sessionResource: SessionResource[F, DB],
      connConfig:      ProjectsConnectionConfig
  ): F[(consumers.EventHandler[F], SubscriptionMechanism[F])] = {
    implicit val sr: SessionResource[F, DB] = sessionResource
    for {
      eventsQueue    <- EventsQueue[F, DB](categoryLock).pure[F]
      handler        <- EventHandler[F](eventsQueue)
      eventProcessor <- EventProcessor[F](connConfig, eventsQueue)
      _              <- EventsDequeueingInitiator(tsReadyCheck, eventsQueue, eventProcessor).initDequeueing
    } yield handler -> SubscriptionMechanism.noOpSubscriptionMechanism(categoryName)
  }
}
