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
import data.dataProjects
import flows.TSProvisioning
import io.circe.Decoder._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.userAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, projects}
import io.renku.jsonld.syntax._
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.Ok
import tooling.{AcceptanceSpec, ApplicationServices}

class UserProjectsResourceSpec extends AcceptanceSpec with ApplicationServices with TSProvisioning {

  private implicit val graph: GraphClass = GraphClass.Default

  Feature("GET knowledge-graph/users/:id/projects to return user's projects") {

    Scenario("User navigates to GET knowledge-graph/users/:id/projects") {

      Given("user has an activated project")

      val userId      = personGitLabIds.generateOne
      val user        = personEntities(fixed(userId.some)).generateOne
      val accessToken = userAccessTokens.generateOne
      gitLabStub.addAuthenticated(userId, accessToken)

      val activatedProject = dataProjects(
        renkuProjectEntities(anyVisibility).modify(replaceMembers(Set(user))).generateOne
      ).generateOne

      val commitId = commitIds.generateOne
      mockCommitDataOnTripleGenerator(activatedProject, activatedProject.entitiesProject.asJsonLD, commitId)
      gitLabStub.setupProject(activatedProject, commitId)
      `data in the Triples Store`(activatedProject, commitId, accessToken)

      And("he has a not activated project")

      val notActivatedProject = dataProjects(
        renkuProjectEntities(visibilityPublic).generateOne
      ).generateOne

      gitLabStub.addProject(notActivatedProject)

      When("user navigates to GET knowledge-graph/users/:id/projects")

      val response = knowledgeGraphClient GET (s"knowledge-graph/users/$userId/projects", accessToken)

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
