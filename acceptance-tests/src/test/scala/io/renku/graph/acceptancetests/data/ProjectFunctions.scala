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

package io.renku.graph.acceptancetests.data

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.cli.CliEntityConverterSyntax._
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.testentities.generators.EntitiesGenerators.replaceProjectCreator
import io.renku.graph.model.tools.JsonLDTools.flattenedJsonLDFrom
import io.renku.graph.model.{entities, persons, testentities}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import monocle.Lens

trait ProjectFunctions {

  def replaceCreatorFrom(creator: testentities.Person, gitLabId: persons.GitLabId): Project => Project = p =>
    p.copy(
      maybeCreator = toProjectMember(creator, gitLabId).some,
      entitiesProject = replaceProjectCreator(creator.some)(p.entitiesProject)
    )

  def addMemberWithId(gitLabId: persons.GitLabId): Project => Project =
    p => p.copy(members = p.members :+ ProjectMember(personNames.generateOne, personUsernames.generateOne, gitLabId))

  def addMemberFrom(person: testentities.Person, gitLabId: persons.GitLabId): Project => Project =
    p => p.copy(members = p.members :+ toProjectMember(person, gitLabId))

  private def memberGitLabId: Lens[ProjectMember, persons.GitLabId] =
    Lens[ProjectMember, persons.GitLabId](_.gitLabId) { newGlId =>
      {
        case m: ProjectMember.ProjectMemberNoEmail   => m.copy(gitLabId = newGlId)
        case m: ProjectMember.ProjectMemberWithEmail => m.copy(gitLabId = newGlId)
      }
    }

  val toPayloadJsonLD: entities.Project => JsonLD = p => {
    val cliProject = p.toCliEntity
    flattenedJsonLDFrom(cliProject.asJsonLD, cliProject.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*)
  }

  private def toProjectMember(p: testentities.Person, gitLabId: persons.GitLabId): ProjectMember =
    ProjectMember(p.name, persons.Username(p.name.show), gitLabId)
}

object ProjectFunctions extends ProjectFunctions
