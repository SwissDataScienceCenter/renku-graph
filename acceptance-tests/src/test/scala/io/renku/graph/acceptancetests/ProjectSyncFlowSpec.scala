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

import cats.syntax.all._
import data._
import db.EventLog
import flows.TSProvisioning
import io.renku.eventlog.TypeSerializers
import io.renku.events.CategoryName
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.testentities.cliShapedPersons
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{removeMembers, renkuProjectEntities, visibilityPublic}
import org.http4s.Status.{NotFound, Ok}
import tooling.{AcceptanceSpec, ApplicationServices}

import java.lang.Thread.sleep
import scala.concurrent.duration._

class ProjectSyncFlowSpec extends AcceptanceSpec with ApplicationServices with TSProvisioning with TypeSerializers {

  Feature("Project info should be kept in sync with GitLab") {

    Scenario("There's a project_path change in GitLab for a repo that is already in KG") {

      val user = authUsers.generateOne
      val testProject =
        renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers()).generateOne
      val project = dataProjects(testProject).map(addMemberWithId(user.id)).generateOne

      Given("repository data in the Triples Store")
      val commitId = commitIds.generateOne
      gitLabStub.addAuthenticated(user)
      gitLabStub.setupProject(project, commitId)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)
      `data in the Triples Store`(project, commitId, user.accessToken)

      Then("the project data should exist in the KG")
      eventually {
        val response = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}")
        response.status                                        shouldBe Ok
        response.jsonBody.hcursor.downField("path").as[String] shouldBe project.path.show.asRight
      }

      When("project_path changes in GitLab")
      val updatedProject = project.copy(entitiesProject = testProject.copy(path = projectPaths.generateOne))
      gitLabStub.replaceProject(updatedProject)
      givenAccessTokenPresentFor(updatedProject, user.accessToken)
      resetTriplesGenerator()
      mockCommitDataOnTripleGenerator(updatedProject, toPayloadJsonLD(updatedProject), commitId)

      And("PROJECT_SYNC event is sent and handled")
      EventLog.forceCategoryEventTriggering(CategoryName("PROJECT_SYNC"), updatedProject.id)
      sleep((2 seconds).toMillis)

      Then("the updated project data should exist in the KG")
      eventually {
        val response = knowledgeGraphClient.GET(s"knowledge-graph/projects/${updatedProject.path}")
        response.status                                        shouldBe Ok
        response.jsonBody.hcursor.downField("path").as[String] shouldBe updatedProject.path.show.asRight
      }

      And("the old project data should be removed")
      knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}").status shouldBe NotFound
    }
  }
}
