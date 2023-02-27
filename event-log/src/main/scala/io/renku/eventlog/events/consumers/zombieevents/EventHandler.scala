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
    override val categoryName: CategoryName,
    zombieStatusCleaner:       ZombieStatusCleaner[F],
    processExecutor:           ProcessExecutor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = ZombieEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = _.event.as[ZombieEvent],
      process = cleanZombieStatus
    )

  private def cleanZombieStatus(event: ZombieEvent): F[Unit] = {
    zombieStatusCleaner.cleanZombieStatus(event) >>= {
      case Updated => updateGauges(event)
      case _       => ().pure[F]
    }
  } recoverWith { case exception => Logger[F].logError(event, exception) }

  private lazy val updateGauges: ZombieEvent => F[Unit] = {
    case ZombieEvent(_, projectPath, GeneratingTriples) =>
      EventStatusGauges[F].awaitingGeneration.increment(projectPath) >>
        EventStatusGauges[F].underGeneration.decrement(projectPath)
    case ZombieEvent(_, projectPath, TransformingTriples) =>
      EventStatusGauges[F].awaitingTransformation.increment(projectPath) >>
        EventStatusGauges[F].underTransformation.decrement(projectPath)
    case ZombieEvent(_, projectPath, Deleting) =>
      EventStatusGauges[F].awaitingDeletion.increment(projectPath) >>
        EventStatusGauges[F].underDeletion.decrement(projectPath)
  }
}

private object EventHandler {
  import eu.timepit.refined.auto._

  def apply[F[_]: Async: SessionResource: Logger: QueriesExecutionTimes: EventStatusGauges]
      : F[consumers.EventHandler[F]] = for {
    zombieStatusCleaner <- ZombieStatusCleaner[F]
    processExecutor     <- ProcessExecutor.concurrent(5)
  } yield new EventHandler[F](categoryName, zombieStatusCleaner, processExecutor)
}
