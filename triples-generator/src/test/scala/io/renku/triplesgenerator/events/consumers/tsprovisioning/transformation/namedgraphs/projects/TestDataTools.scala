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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.projects

import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import io.renku.graph.model.entities

private object TestDataTools {

  def toProjectMutableData(project: entities.Project): ProjectMutableData = ProjectMutableData(
    project.name,
    Nel.of(project.dateCreated),
    List(project.dateModified),
    findParent(project),
    project.visibility,
    project.maybeDescription,
    project.keywords,
    findAgent(project),
    project.maybeCreator.map(_.resourceId),
    project.images.sortBy(_.position).map(_.uri)
  )

  def findParent(project: entities.Project) = project match {
    case p: entities.Project with entities.Parent => p.parentResourceId.some
    case _ => None
  }

  def findAgent(project: entities.Project) = project match {
    case p: entities.RenkuProject => p.agent.some
    case _ => None
  }
}
