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
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.{TSData, dataProjects}
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectSchemaVersions
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{personEntities, renkuProjectEntities, visibilityPublic}
import io.renku.graph.model.{SchemaVersion, testentities}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.scalactic.source.Position
import org.scalatest.enablers.Retrying
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.time.{Minutes, Seconds, Span}
import org.scalatest.{Assertion, GivenWhenThen}

import java.lang.Thread.sleep
import scala.concurrent.duration._

class ProjectReProvisioningSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with TSProvisioning
    with TSData {

  Feature("Project re-provisioning") {

    Scenario("Project related data in the TS should be regenerated on-demand") {

      import TestData._

      Given("there's data for the project in the TS")

      val project  = dataProjects(testEntitiesProject).generateOne
      val commitId = commitIds.generateOne

      `GET <gitlabApi>/user returning OK`(user)
      mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commitId)

      `data in the Triples Store`(project, commitId)

      eventually {
        knowledgeGraphClient
          .GET(s"knowledge-graph/projects/${project.path}", accessToken)
          .jsonBody
          .as[Json]
          .flatMap(_.hcursor.downField("version").as[SchemaVersion]) shouldBe project.entitiesProject.version.asRight
      }

      When("there's different data produced during Triples Generation")
      resetTriplesGenerator()
      val newProjectVersion = projectSchemaVersions.generateOne
      `GET <triples-generator>/projects/:id/commits/:id returning OK`(
        project,
        commitId,
        replace(newProjectVersion)(project.entitiesProject).asJsonLD
      )

      And("a CLEAN_UP_REQUEST event is send to EL")
      eventLogClient.sendEvent(json"""{
        "categoryName": "CLEAN_UP_REQUEST",
        "project": {
          "path": ${project.entitiesProject.path.value}
        }
      }""")

      Then("the old data in the TS should be replaced with the new")
      sleep((10 seconds).toMillis)
      `wait for events to be processed`(project.id)

      eventually {
        knowledgeGraphClient
          .GET(s"knowledge-graph/projects/${project.path}", accessToken)
          .jsonBody
          .as[Json]
          .flatMap(_.hcursor.downField("version").as[SchemaVersion]) shouldBe newProjectVersion.asRight
      }(PatienceConfig(timeout = Span(20, Minutes), interval = Span(10, Seconds)),
        implicitly[Retrying[Assertion]],
        implicitly[Position]
      )
    }
  }

  private object TestData {

    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    val testEntitiesProject = renkuProjectEntities(visibilityPublic).generateOne
      .copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some)))
  }

  private def replace(version: SchemaVersion): testentities.RenkuProject => testentities.RenkuProject = {
    case p: testentities.RenkuProject.WithoutParent => p.copy(version = version)
    case p: testentities.RenkuProject.WithParent    => p.copy(version = version)
  }
}
