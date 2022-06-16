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

package io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.datasets

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.entities.{Dataset, Project}
import io.renku.triplesgenerator.events.categories.tsprovisioning.ProjectFunctions
import io.renku.triplesgenerator.events.categories.tsprovisioning.TransformationStep.Queries

private trait HierarchyOnInvalidationUpdater[F[_]] {
  def updateHierarchyOnInvalidation: ((Project, Queries)) => F[(Project, Queries)]
}

private object HierarchyOnInvalidationUpdater {
  def apply[F[_]: MonadThrow]: F[HierarchyOnInvalidationUpdater[F]] =
    new HierarchyOnInvalidationUpdaterImpl[F](UpdatesCreator, ProjectFunctions).pure[F].widen
}

private class HierarchyOnInvalidationUpdaterImpl[F[_]: MonadThrow](updatesCreator: UpdatesCreator,
                                                                   projectFunctions: ProjectFunctions
) extends HierarchyOnInvalidationUpdater[F] {
  import projectFunctions._
  import updatesCreator._

  override def updateHierarchyOnInvalidation: ((Project, Queries)) => F[(Project, Queries)] = {
    case (project, queries) =>
      findInvalidatedDatasets(project)
        .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, dataset) =>
          projectAndQueriesF map { case (project, queries) =>
            val preDataUploadQueries = dataset.provenance match {
              case _: Dataset.Provenance.Internal =>
                prepareUpdatesWhenInvalidated(dataset.asInstanceOf[Dataset[Dataset.Provenance.Internal]])
              case _: Dataset.Provenance.ImportedExternal =>
                prepareUpdatesWhenInvalidated(dataset.asInstanceOf[Dataset[Dataset.Provenance.ImportedExternal]])
              case _: Dataset.Provenance.ImportedInternal =>
                prepareUpdatesWhenInvalidated(dataset.asInstanceOf[Dataset[Dataset.Provenance.ImportedInternal]])
              case _ => Nil
            }
            project -> (queries |+| Queries.postDataQueriesOnly(preDataUploadQueries))
          }
        }
  }
}
