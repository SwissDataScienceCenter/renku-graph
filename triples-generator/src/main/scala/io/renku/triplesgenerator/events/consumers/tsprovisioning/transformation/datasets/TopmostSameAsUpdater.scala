/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.datasets

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.consumers.tsprovisioning.ProjectFunctions
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries
import org.typelevel.log4cats.Logger

private trait TopmostSameAsUpdater[F[_]] {
  def updateTopmostSameAs: ((Project, Queries)) => F[(Project, Queries)]
}

private object TopmostSameAsUpdater {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[TopmostSameAsUpdater[F]] = for {
    kgDatasetInfoFinder <- KGDatasetInfoFinder[F]
  } yield new TopmostSameAsUpdaterImpl[F](kgDatasetInfoFinder, UpdatesCreator, ProjectFunctions)
}

private class TopmostSameAsUpdaterImpl[F[_]: MonadThrow](
    kgDatasetInfoFinder: KGDatasetInfoFinder[F],
    updatesCreator:      UpdatesCreator,
    projectFunctions:    ProjectFunctions
) extends TopmostSameAsUpdater[F] {

  import kgDatasetInfoFinder._
  import projectFunctions._
  import updatesCreator._

  override def updateTopmostSameAs: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    findInternallyImportedDatasets(project)
      .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, dataset) =>
        for {
          projectAndQueries        <- projectAndQueriesF
          maybeParentTopmostSameAs <- findParentTopmostSameAs(dataset.provenance.sameAs)
          maybeKGTopmostSameAses   <- findTopmostSameAs(dataset.identification.resourceId)
          updatedDataset = maybeParentTopmostSameAs.map(dataset.update) getOrElse dataset
        } yield (
          update(dataset, updatedDataset)(projectAndQueries._1),
          projectAndQueries._2 |+| Queries.postDataQueriesOnly(
            prepareUpdates(dataset, maybeKGTopmostSameAses) :::
              prepareTopmostSameAsCleanup(updatedDataset, maybeParentTopmostSameAs)
          )
        )
      }
  }
}
