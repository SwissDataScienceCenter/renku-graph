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

package io.renku.eventlog.events.consumers.zombieevents

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.{consumers, CategoryName}
import io.renku.events.consumers.ProcessExecutor
import io.renku.graph.model.events.EventStatus._
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger: EventStatusGauges](
    zombieStatusCleaner:       ZombieStatusCleaner[F],
    processExecutor:           ProcessExecutor[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = ZombieEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[ZombieEvent],
      process = ev => Logger[F].info(show"$categoryName: $ev accepted") >> cleanZombieStatus(ev)
    )

  private def cleanZombieStatus(event: ZombieEvent): F[Unit] = {
    zombieStatusCleaner.cleanZombieStatus(event) >>= {
      case Updated => updateGauges(event)
      case _       => ().pure[F]
    }
  } recoverWith { case exception =>
    Logger[F].error(exception)(show"$categoryName: $event -> Failure")
  }

  private lazy val updateGauges: ZombieEvent => F[Unit] = {
    case ZombieEvent(_, projectSlug, GeneratingTriples) =>
      EventStatusGauges[F].awaitingGeneration.increment(projectSlug) >>
        EventStatusGauges[F].underGeneration.decrement(projectSlug)
    case ZombieEvent(_, projectSlug, TransformingTriples) =>
      EventStatusGauges[F].awaitingTransformation.increment(projectSlug) >>
        EventStatusGauges[F].underTransformation.decrement(projectSlug)
    case ZombieEvent(_, projectSlug, Deleting) =>
      EventStatusGauges[F].awaitingDeletion.increment(projectSlug) >>
        EventStatusGauges[F].underDeletion.decrement(projectSlug)
  }
}

private object EventHandler {
  import eu.timepit.refined.auto._

  def apply[F[_]: Async: SessionResource: Logger: QueriesExecutionTimes: EventStatusGauges]
      : F[consumers.EventHandler[F]] = for {
    zombieStatusCleaner <- ZombieStatusCleaner[F]
    processExecutor     <- ProcessExecutor.concurrent(5)
  } yield new EventHandler[F](zombieStatusCleaner, processExecutor)
}
