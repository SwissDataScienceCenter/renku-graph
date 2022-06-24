/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.eventlog.TypeSerializers
import io.renku.events.CategoryName
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.dataProjects
import io.renku.graph.acceptancetests.db.EventLog
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{renkuProjectEntities, visibilityPublic}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.http4s.Status.{NotFound, Ok}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

import java.lang.Thread.sleep
import scala.concurrent.duration._

class ProjectSyncFlowSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with TypeSerializers {

  Feature("Project info should be kept in sync with GitLab") {

    Scenario("There's a project_path change in GitLab for a repo that is already in KG") {

      implicit val accessToken: AccessToken = accessTokens.generateOne
      val testEntitiesProject = renkuProjectEntities(visibilityPublic).generateOne
      val project             = dataProjects(testEntitiesProject).generateOne

      Given("repository data in the RDF Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commitId)
      `data in the RDF store`(project, commitId)

      Then("the project data should exist in the KG")
      eventually {
        val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}")
        projectDetailsResponse.status                                        shouldBe Ok
        projectDetailsResponse.jsonBody.hcursor.downField("path").as[String] shouldBe project.path.show.asRight
      }

      When("project_path changes in GitLab")
      resetGitLab()
      resetTriplesGenerator()
      val updatedProject = project.copy(entitiesProject = testEntitiesProject.copy(path = projectPaths.generateOne))
      mockDataOnGitLabAPIs(updatedProject, updatedProject.entitiesProject.asJsonLD, commitId)
      givenAccessTokenPresentFor(updatedProject)

      And("PROJECT_SYNC event is sent and handled")
      EventLog.forceCategoryEventTriggering(CategoryName("PROJECT_SYNC"), updatedProject.id)
      sleep((2 seconds).toMillis)

      Then("the updated project data should exist in the KG")
      eventually {
        val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${updatedProject.path}")
        projectDetailsResponse.status                                        shouldBe Ok
        projectDetailsResponse.jsonBody.hcursor.downField("path").as[String] shouldBe updatedProject.path.show.asRight
      }

      And("the old project data should be removed")
      knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}").status shouldBe NotFound
    }
  }
}
