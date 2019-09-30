/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets

import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.model.users.{Email, Name => UserName}

object model {

  final case class Dataset(id:               Identifier,
                           name:             Name,
                           maybeDescription: Option[Description],
                           created:          DatasetCreation,
                           published:        DatasetPublishing,
                           part:             List[DatasetPart],
                           project:          List[DatasetProject])

  final case class DatasetCreation(date: DateCreated, agent: DatasetAgent)
  final case class DatasetAgent(email:   Email, name:        UserName)

  final case class DatasetPublishing(maybeDate: Option[PublishedDate], creators: Set[DatasetCreator])
  final case class DatasetCreator(maybeEmail:   Option[Email], name:             UserName)

  final case class DatasetPart(name: PartName, atLocation: PartLocation, dateCreated: PartDateCreated)

  final case class DatasetProject(path: ProjectPath, name: projects.Name)
}
