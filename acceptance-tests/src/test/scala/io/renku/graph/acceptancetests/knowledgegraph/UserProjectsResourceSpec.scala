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

package io.renku.graph.acceptancetests
package knowledgegraph

import cats.syntax.all._
import data._
import flows.TSProvisioning
import io.circe.Decoder._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.projects
import io.renku.graph.model.testentities._
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.Ok
import tooling.{AcceptanceSpec, ApplicationServices}

class UserProjectsResourceSpec extends AcceptanceSpec with ApplicationServices with TSProvisioning {

  Feature("GET knowledge-graph/users/:id/projects to return user's projects") {

    Scenario("User navigates to GET knowledge-graph/users/:id/projects") {

      Given("user has an activated project")

      val user = authUsers.generateOne
      gitLabStub.addAuthenticated(user.id, user.accessToken)

      val activatedProject = dataProjects(
        renkuProjectEntities(anyVisibility, creatorGen = cliShapedPersons).modify(removeMembers()).generateOne
      ).map(addMemberWithId(user.id)).generateOne

      val commitId = commitIds.generateOne
      mockCommitDataOnTripleGenerator(activatedProject, toPayloadJsonLD(activatedProject), commitId)
      gitLabStub.setupProject(activatedProject, commitId)
      `data in the Triples Store`(activatedProject, commitId, user.accessToken)

      And("he has a not activated project")
      val notActivatedProject = dataProjects(
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers()).generateOne
      ).map(addMemberWithId(user.id)).generateOne

      gitLabStub.addProject(notActivatedProject)

      When("user navigates to GET knowledge-graph/users/:id/projects")

      val response = knowledgeGraphClient GET (s"knowledge-graph/users/${user.id}/projects", user.accessToken)

      Then("he should get OK response with all the projects")
      response.status shouldBe Ok

      val Right(foundProjectsInJson) = response.jsonBody.as[List[Json]]
      foundProjectsInJson
        .map(_.hcursor.downField("name").as[projects.Name])
        .sequence
        .fold(fail(_), identity) shouldBe List(activatedProject.name, notActivatedProject.name)
        .sortBy(_.value.toLowerCase)
    }
  }
}
