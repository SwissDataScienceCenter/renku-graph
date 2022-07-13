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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.projects

import cats.syntax.all._
import io.renku.graph.model.entities.{NonRenkuProject, Project, RenkuProject}
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries.preDataQueriesOnly

private trait DateCreatedUpdater {
  def updateDateCreated(kgData: ProjectMutableData): ((Project, Queries)) => (Project, Queries)
}

private object DateCreatedUpdater {
  def apply(): DateCreatedUpdater = new DateCreatedUpdaterImpl(UpdatesCreator)
}

private class DateCreatedUpdaterImpl(updatesCreator: UpdatesCreator) extends DateCreatedUpdater {
  import updatesCreator._

  override def updateDateCreated(kgData: ProjectMutableData): ((Project, Queries)) => (Project, Queries) = {
    case (project, queries) if (project.dateCreated compareTo kgData.dateCreated) < 0 =>
      project -> (queries |+| preDataQueriesOnly(dateCreatedDeletion(project, kgData)))
    case (project, queries) if (project.dateCreated compareTo kgData.dateCreated) > 0 =>
      val updatedProj = project match {
        case p: RenkuProject.WithoutParent    => p.copy(dateCreated = kgData.dateCreated)
        case p: RenkuProject.WithParent       => p.copy(dateCreated = kgData.dateCreated)
        case p: NonRenkuProject.WithoutParent => p.copy(dateCreated = kgData.dateCreated)
        case p: NonRenkuProject.WithParent    => p.copy(dateCreated = kgData.dateCreated)
      }
      updatedProj -> queries
    case (project, queries) => project -> queries
  }
}
