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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.datasets

import cats.Monad
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.triplesgenerator.tsprovisioning.TransformationStep.Queries
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait PublicationEventsUpdater[F[_]] {
  def updatePublicationEvents: ((Project, Queries)) => F[(Project, Queries)]
}

private object PublicationEventsUpdater {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[PublicationEventsUpdater[F]] =
    KGDatasetInfoFinder[F].map(new PublicationEventsUpdaterImpl[F](_, UpdatesCreator))
}

private class PublicationEventsUpdaterImpl[F[_]: Monad](kgDatasetInfoFinder: KGDatasetInfoFinder[F],
                                                        updatesCreator: UpdatesCreator
) extends PublicationEventsUpdater[F] {

  import kgDatasetInfoFinder.checkPublicationEventsExist

  override def updatePublicationEvents: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    project.datasets
      .map { ds =>
        checkPublicationEventsExist(project.resourceId, ds.resourceId).map {
          case false if ds.publicationEvents.isEmpty => List.empty[SparqlQuery]
          case _                                     => updatesCreator.deletePublicationEvents(project.resourceId, ds)
        }
      }
      .sequence
      .map(_.flatten)
      .map(preUpdateQueries => project -> (queries ++ Queries.preDataQueriesOnly(preUpdateQueries)))
  }
}
