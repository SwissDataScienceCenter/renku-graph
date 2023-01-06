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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.datasets

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.{Dataset, Project}
import io.renku.graph.model.projects
import io.renku.triplesgenerator.events.consumers.tsprovisioning.ProjectFunctions
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries
import io.renku.triplesstore.{SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait HierarchyOnInvalidationUpdater[F[_]] {
  def updateHierarchyOnInvalidation: ((Project, Queries)) => F[(Project, Queries)]
}

private object HierarchyOnInvalidationUpdater {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[HierarchyOnInvalidationUpdater[F]] =
    KGDatasetInfoFinder[F]
      .map(new HierarchyOnInvalidationUpdaterImpl[F](_, UpdatesCreator, ProjectFunctions))
}

private class HierarchyOnInvalidationUpdaterImpl[F[_]: MonadThrow](kgDatasetInfoFinder: KGDatasetInfoFinder[F],
                                                                   updatesCreator:   UpdatesCreator,
                                                                   projectFunctions: ProjectFunctions
) extends HierarchyOnInvalidationUpdater[F] {
  import kgDatasetInfoFinder.findWhereNotInvalidated
  import projectFunctions._
  import updatesCreator._

  override def updateHierarchyOnInvalidation: ((Project, Queries)) => F[(Project, Queries)] = {
    case (project, queries) =>
      findInvalidatedDatasets(project)
        .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, dataset) =>
          projectAndQueriesF >>= { case (project, queries) =>
            findWhereNotInvalidated(dataset.resourceId).map(_.filterNot(_ == project.resourceId)).map {
              case projects if projects.nonEmpty =>
                project -> queries
              case _ =>
                val preDataUploadQueries = prepareQueries(project.resourceId, dataset)
                project -> (queries |+| Queries.postDataQueriesOnly(preDataUploadQueries))
            }
          }
        }
  }

  private def prepareQueries(projectId: projects.ResourceId, dataset: Dataset[Dataset.Provenance]): List[SparqlQuery] =
    dataset.provenance match {
      case _: Dataset.Provenance.Internal =>
        prepareUpdatesWhenInvalidated(dataset.asInstanceOf[Dataset[Dataset.Provenance.Internal]])
      case _: Dataset.Provenance.ImportedExternal =>
        prepareUpdatesWhenInvalidatedExt(projectId, dataset.asInstanceOf[Dataset[Dataset.Provenance.ImportedExternal]])
      case _: Dataset.Provenance.ImportedInternal =>
        prepareUpdatesWhenInvalidatedInt(projectId, dataset.asInstanceOf[Dataset[Dataset.Provenance.ImportedInternal]])
      case _ => Nil
    }
}
