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

package ch.datascience.triplesgenerator.events.categories.membersync

import ch.datascience.graph.Schemas.{rdf, schema}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.ResourceId
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import eu.timepit.refined.auto._

private class UpdatesCreator(renkuBaseUrl: RenkuBaseUrl) {

  def insertion(projectPath: projects.Path, members: Set[GitLabProjectMember]): List[SparqlQuery] = ???

  def removal(projectPath: projects.Path, members: Set[KGProjectMember]): SparqlQuery =
    SparqlQuery.of(
      name = "unlink project members",
      Prefixes.of(schema -> "schema", rdf -> "rdf"),
      s"""|DELETE DATA { 
          |  ${generateTriples(projectPath, members).mkString("\n")} 
          |}
          |""".stripMargin
    )

  private def generateTriples(projectPath: projects.Path, members: Set[KGProjectMember]) =
    members map { member =>
      s"${ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]} schema:member ${member.id.showAs[RdfResource]}."
    }
}
