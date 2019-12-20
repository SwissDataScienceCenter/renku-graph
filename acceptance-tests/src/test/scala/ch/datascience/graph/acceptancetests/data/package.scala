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

package ch.datascience.graph.acceptancetests

import ch.datascience.config.renku
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.tooling.RDFStore
import ch.datascience.graph.model.EventsGenerators.projectIds
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{ProjectId, Project => GitLabProject}
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.FusekiBaseUrl

package object data {
  val currentSchemaVersion:   SchemaVersion      = SchemaVersion("0.5.0")
  val renkuResourcesUrl:      renku.ResourcesUrl = renku.ResourcesUrl("http://localhost:9004/knowledge-graph")
  implicit val fusekiBaseUrl: FusekiBaseUrl      = RDFStore.fusekiBaseUrl

  implicit class ProjectOps(project: Project) {

    def toGitLabProject(id: ProjectId = projectIds.generateOne): GitLabProject = GitLabProject(
      id,
      project.path
    )
  }
}
