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
import io.renku.cli.model.CliProject
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.replaceProjectCreator
import io.renku.graph.model._
import io.renku.graph.model.gitlab.GitLabMember
import io.renku.graph.model.projects.Role
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._

trait ProjectFunctions {

  def replaceCreatorFrom(creator: testentities.Person, gitLabId: persons.GitLabId): Project => Project = p =>
    p.copy(
      maybeCreator = toProjectMember(creator, gitLabId, Role.Owner).user.some,
      entitiesProject = replaceProjectCreator(creator.some)(p.entitiesProject)
    )

  def addMemberWithId(gitLabId: persons.GitLabId, role: Role): Project => Project =
    p =>
      p.copy(members =
        p.members :+ GitLabMember(
          personNames.generateOne,
          personUsernames.generateOne,
          gitLabId,
          None,
          Role.toGitLabAccessLevel(role)
        )
      )

  def addMemberFrom(person: testentities.Person, gitLabId: persons.GitLabId, role: Role): Project => Project =
    p => p.copy(members = p.members :+ toProjectMember(person, gitLabId, role))

  def toPayloadJsonLD(p: Project)(implicit renkuUrl: RenkuUrl): JsonLD =
    toPayloadJsonLD(p.entitiesProject)

  def toPayloadJsonLD(p: testentities.RenkuProject)(implicit renkuUrl: RenkuUrl): JsonLD =
    p.to[CliProject].asJsonLD(CliProject.flatJsonLDEncoder)

  private def toProjectMember(p: testentities.Person, gitLabId: persons.GitLabId, role: Role): GitLabMember =
    GitLabMember(p.name, persons.Username(p.name.show), gitLabId, None, Role.toGitLabAccessLevel(role))
}

object ProjectFunctions extends ProjectFunctions
