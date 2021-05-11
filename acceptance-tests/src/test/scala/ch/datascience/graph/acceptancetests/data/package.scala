/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests

import ch.datascience.config.renku
import ch.datascience.graph.acceptancetests.tooling.RDFStore
import ch.datascience.graph.model.{CliVersion, SchemaVersion}
import ch.datascience.rdfstore.{FusekiBaseUrl, entities}
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.knowledgegraph.projects.model
import ch.datascience.knowledgegraph.projects.model.{Creator, Project}
import ch.datascience.rdfstore.entities.Person

package object data {
  val currentVersionPair:     RenkuVersionPair   = RenkuVersionPair(CliVersion("0.12.2"), SchemaVersion("8"))
  val renkuResourcesUrl:      renku.ResourcesUrl = renku.ResourcesUrl("http://localhost:9004/knowledge-graph")
  implicit val fusekiBaseUrl: FusekiBaseUrl      = RDFStore.fusekiBaseUrl

  implicit class ProjectCreatorOps(creator: Creator) {
    def toEntitiesPerson: entities.Person = entities.Person(creator.name, creator.maybeEmail)
  }

  implicit class ProjectOps(project: Project) {
    def toEntitiesProject(cliVersion: CliVersion): entities.Project = entities.Project(
      project.path,
      project.name,
      cliVersion,
      project.created.date,
      project.created.maybeCreator.map(_.toEntitiesPerson),
      None,
      project.created.maybeCreator.map(_.toEntitiesPerson).toList.toSet,
      maybeParentProject = project.forking.maybeParent.map(parent =>
        entities.Project(
          parent.path,
          parent.name,
          cliVersion,
          parent.created.date,
          parent.created.maybeCreator.map(_.toEntitiesPerson),
          None,
          parent.created.maybeCreator.map(_.toEntitiesPerson).toList.toSet,
          maybeParentProject = None,
          project.version
        )
      ),
      project.version
    )
  }
}
