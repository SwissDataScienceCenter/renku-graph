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

package io.renku.graph.acceptancetests

import cats.effect.IO
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.acceptancetests.tooling.ResponseTools.ResponseOps
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{SchemaVersion, testentities}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.http4s.Response
import org.http4s.Status.Ok
import org.scalactic.source.Position
import org.scalatest.GivenWhenThen
import org.scalatest.enablers.Retrying
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.time.{Minutes, Seconds, Span}

class ReProvisioningSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with RdfStoreData {

  Feature("ReProvisioning") {

    Scenario("Update CLI version and expect re-provisioning") {
      import TestData._

      Given("The TG is using an older version of the CLI")

      And("There is data from this version in Jena")

      val project  = dataProjects(testEntitiesProject).generateOne
      val commitId = commitIds.generateOne

      `data in the RDF store`(project, project.entitiesProject.asJsonLD, commitId)

      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)
      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      projectDetailsResponseIsValid(projectDetailsResponse, initialProjectSchemaVersion)

      val newSchemaVersion = SchemaVersion(nonEmptyStrings().generateOne)
      val testEntitiesProjectWithNewSchemaVersion = project.entitiesProject match {
        case p: testentities.ProjectWithParent    => p.copy(version = newSchemaVersion)
        case p: testentities.ProjectWithoutParent => p.copy(version = newSchemaVersion)
      }

      `GET <triples-generator>/projects/:id/commits/:id returning OK`(project,
                                                                      commitId,
                                                                      testEntitiesProjectWithNewSchemaVersion.asJsonLD
      )

      When("The compatibility matrix is updated and the TG is restarted")
      restartTGWithNewCompatMatrix()

      Then("Re-provisioning is triggered")
      And("The new data can be queried in Jena")

      `GET <gitlabApi>/projects/:path AND :id returning OK with`(project)

      eventually {
        val updatedProjectDetailsResponse =
          knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)
        projectDetailsResponseIsValid(updatedProjectDetailsResponse, newSchemaVersion)

      }(PatienceConfig(timeout = Span(20, Minutes), interval = Span(10, Seconds)),
        Retrying.retryingNatureOfT,
        Position.here
      )
    }
  }

  private object TestData {

    implicit val accessToken: AccessToken = accessTokens.generateOne
    val initialProjectSchemaVersion = SchemaVersion("8")

    val testEntitiesProject = projectEntities(visibilityPublic)
      .map(_.copy(version = initialProjectSchemaVersion))
      .withActivities(activityEntities(planEntities()))
      .generateOne
  }

  private def projectDetailsResponseIsValid(projectDetailsResponse:       Response[IO],
                                            expectedProjectSchemaVersion: SchemaVersion
  ) = {
    projectDetailsResponse.status shouldBe Ok
    val Right(projectDetails)       = projectDetailsResponse.bodyAsJson.as[Json]
    val Right(projectSchemaVersion) = projectDetails.hcursor.downField("version").as[String]
    projectSchemaVersion shouldBe expectedProjectSchemaVersion.value
  }

  private def restartTGWithNewCompatMatrix(): Unit = {
    val newTriplesGenerator = triplesGenerator.copy(
      serviceArgsList = List(() => "application-re-provisioning.conf"),
      preServiceStart = List()
    )
    stop(triplesGenerator)
    run(newTriplesGenerator)
  }
}
