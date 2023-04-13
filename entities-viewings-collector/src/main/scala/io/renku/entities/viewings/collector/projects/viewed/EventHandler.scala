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

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.{CategoryName, consumers}
import io.renku.events.consumers.ProcessExecutor
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private class EventHandler[F[_]: MonadCancelThrow: Logger](
    eventPersister:            EventPersister[F],
    processExecutor:           ProcessExecutor[F],
    override val categoryName: CategoryName = categoryName
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = ProjectViewedEvent

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      _.event.as[ProjectViewedEvent],
      process
    )

  private def process(event: Event) =
    Logger[F].info(show"$categoryName: $event accepted") >>
      eventPersister.persist(event)
}

private object EventHandler {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connConfig: ProjectsConnectionConfig
  ): F[consumers.EventHandler[F]] =
    (EventPersister[F](connConfig), ProcessExecutor.concurrent(processesCount = 100))
      .mapN(new EventHandler[F](_, _))
}
