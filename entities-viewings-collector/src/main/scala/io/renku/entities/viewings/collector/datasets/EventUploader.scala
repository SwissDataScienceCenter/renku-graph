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

package io.renku.entities.viewings.collector.datasets

import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import cats.data.OptionT
import io.renku.entities.viewings.collector.projects.viewed.EventPersister
import io.renku.graph.model.projects
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private trait EventUploader[F[_]] {
  def upload(event: DatasetViewedEvent): F[Unit]
}

private object EventUploader {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[EventUploader[F]] =
    (ProjectFinder[F], EventPersister[F])
      .mapN(new EventUploaderImpl[F](_, _))
}

private class EventUploaderImpl[F[_]: MonadThrow](
    projectFinder:  ProjectFinder[F],
    eventPersister: EventPersister[F]
) extends EventUploader[F] {

  import eventPersister._
  import projectFinder._

  override def upload(event: DatasetViewedEvent): F[Unit] =
    OptionT(findProject(event.identifier))
      .map(ProjectViewedEvent(_, projects.DateViewed(event.dateViewed.value), event.maybeUserId))
      .semiflatMap(persist)
      .value
      .void
}
