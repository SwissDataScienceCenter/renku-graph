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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.datasets

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.triplesgenerator.events.categories.triplesgenerated.ProjectFunctions
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.Queries

private trait DerivationHierarchyUpdater[F[_]] {
  def fixDerivationHierarchies: ((Project, Queries)) => F[(Project, Queries)]
}

private object DerivationHierarchyUpdater {
  def apply[F[_]: MonadThrow]: F[DerivationHierarchyUpdater[F]] =
    new DerivationHierarchyUpdaterImpl[F](UpdatesCreator, ProjectFunctions).pure[F].widen
}

private class DerivationHierarchyUpdaterImpl[F[_]: MonadThrow](
    updatesCreator:   UpdatesCreator,
    projectFunctions: ProjectFunctions
) extends DerivationHierarchyUpdater[F] {
  import projectFunctions._
  import updatesCreator._

  override def fixDerivationHierarchies: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    findModifiedDatasets(project)
      .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, originalDS) =>
        projectAndQueriesF >>= { case (proj, quers) =>
          findTopmostDerivedFrom[F](originalDS, proj)
            .map(originalDS.update)
            .map(updatedDS => update(originalDS, updatedDS)(proj) -> updatedDS)
            .map { case (updatedProj, updatedDS) =>
              val derivedFromDeletions = deleteOtherDerivedFrom(updatedDS)
              val topmostDeletions     = deleteOtherTopmostDerivedFrom(updatedDS)
              updatedProj -> (quers |+| Queries.postDataQueriesOnly(derivedFromDeletions ::: topmostDeletions))
            }
        }
      }
  }
}
