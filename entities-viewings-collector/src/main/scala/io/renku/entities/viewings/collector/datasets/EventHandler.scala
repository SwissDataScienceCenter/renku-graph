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

package io.renku.entities.viewings.collector.datasets

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.{CategoryName, consumers}
import io.renku.events.consumers.{EventSchedulingResult, ProcessExecutor}
import io.renku.triplesgenerator.api.events.DatasetViewedEvent
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    precondition:              F[Option[EventSchedulingResult]],
    eventUploader:             EventUploader[F],
    processExecutor:           ProcessExecutor[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = DatasetViewedEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      _.event.as[DatasetViewedEvent],
      process,
      precondition
    )

  private def process(event: Event) =
    Logger[F].info(show"$categoryName: $event accepted") >>
      eventUploader.upload(event)
}

private object EventHandler {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      precondition: F[Option[EventSchedulingResult]],
      connConfig:   ProjectsConnectionConfig
  ): F[consumers.EventHandler[F]] =
    (EventUploader[F](connConfig), ProcessExecutor.concurrent(processesCount = 100))
      .mapN(new EventHandler[F](precondition, _, _))
}
