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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.projects

import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.triplesgenerator.tsprovisioning.TransformationStep.Queries
import io.renku.triplesgenerator.tsprovisioning.TransformationStep.Queries.preDataQueriesOnly

private trait DateModifiedUpdater {
  def updateDateModified(kgData: ProjectMutableData): ((Project, Queries)) => (Project, Queries)
}

private object DateModifiedUpdater {
  def apply(): DateModifiedUpdater = new DateModifiedUpdaterImpl(UpdatesCreator)
}

private class DateModifiedUpdaterImpl(updatesCreator: UpdatesCreator) extends DateModifiedUpdater {
  import updatesCreator._

  override def updateDateModified(tsData: ProjectMutableData): ((Project, Queries)) => (Project, Queries) = {
    case (project, queries) if tsData.maybeMaxDateModified.exists(_ < project.dateModified) =>
      project -> (queries |+| preDataQueriesOnly(dateModifiedDeletion(project, tsData)))
    case (project, queries) if tsData.maybeMaxDateModified.exists(_ > project.dateModified) =>
      val newDate = (p: Project) => tsData.maybeMaxDateModified.getOrElse(p.dateModified)
      val updatedProj = project.fold(
        p => p.copy(dateModified = newDate(p)),
        p => p.copy(dateModified = newDate(p)),
        p => p.copy(dateModified = newDate(p)),
        p => p.copy(dateModified = newDate(p))
      )
      updatedProj -> queries
    case (project, queries) => project -> queries
  }
}
