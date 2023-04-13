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

package io.renku.entities.viewings.collector
package datasets

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.viewings.collector.persons.{GLUserViewedDataset, PersonViewedDatasetPersister}
import io.renku.entities.viewings.collector.projects.viewed.EventPersister
import io.renku.graph.model.projects
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent, UserId}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

trait EventUploader[F[_]] {
  def upload(event: DatasetViewedEvent): F[Unit]
}

object EventUploader {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](connConfig: ProjectsConnectionConfig): F[EventUploader[F]] =
    (DSInfoFinder[F](connConfig), EventPersister[F](connConfig), PersonViewedDatasetPersister[F](connConfig))
      .mapN(new EventUploaderImpl[F](_, _, _))

  def apply[F[_]: MonadThrow](tsClient: TSClient[F]): EventUploader[F] =
    new EventUploaderImpl[F](DSInfoFinder[F](tsClient),
                             EventPersister[F](tsClient),
                             PersonViewedDatasetPersister[F](tsClient)
    )
}

private class EventUploaderImpl[F[_]: MonadThrow](
    dsInfoFinder:                 DSInfoFinder[F],
    projectViewedEventPersister:  EventPersister[F],
    personViewedDatasetPersister: PersonViewedDatasetPersister[F]
) extends EventUploader[F] {

  import dsInfoFinder._

  override def upload(event: DatasetViewedEvent): F[Unit] =
    OptionT(findDSInfo(event.identifier))
      .semiflatTap(persistProjectViewedEvent(_, event))
      .semiflatTap(persistPersonViewedDataset(_, event))
      .value
      .void

  private def persistProjectViewedEvent(dsInfo: DSInfo, event: DatasetViewedEvent) =
    projectViewedEventPersister.persist(
      ProjectViewedEvent(dsInfo.projectPath,
                         projects.DateViewed(event.dateViewed.value),
                         event.maybeUserId.map(UserId(_))
      )
    )

  private def persistPersonViewedDataset(dsInfo: DSInfo, event: DatasetViewedEvent) =
    event.maybeUserId match {
      case None => ().pure[F]
      case Some(glId) =>
        personViewedDatasetPersister.persist(
          GLUserViewedDataset(UserId(glId), dsInfo.dataset, event.dateViewed)
        )
    }
}
