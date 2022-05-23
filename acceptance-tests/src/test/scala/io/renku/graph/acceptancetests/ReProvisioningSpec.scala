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
import io.renku.config.ServiceVersion
import io.renku.generators.CommonGraphGenerators.{authUsers, serviceVersions}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.{GraphServices, ServiceClient}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{SchemaVersion, testentities}
import io.renku.http.client.AccessToken
import io.renku.jsonld.syntax._
import org.http4s.Status.Ok
import org.scalactic.source.Position
import org.scalatest.GivenWhenThen
import org.scalatest.enablers.Retrying
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.time.{Minutes, Seconds, Span}

import java.nio.file.{Files, Paths}

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

      `GET <gitlabApi>/user returning OK`(user)
      mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commitId)

      `data in the RDF store`(project, commitId)

      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      projectDetailsResponseIsValid(projectDetailsResponse, initialProjectSchemaVersion)

      val newSchemaVersion = SchemaVersion(nonEmptyStrings().generateOne)
      val testEntitiesProjectWithNewSchemaVersion = project.entitiesProject match {
        case p: testentities.RenkuProject.WithParent    => p.copy(version = newSchemaVersion)
        case p: testentities.RenkuProject.WithoutParent => p.copy(version = newSchemaVersion)
      }

      `GET <triples-generator>/projects/:id/commits/:id returning OK`(project,
                                                                      commitId,
                                                                      testEntitiesProjectWithNewSchemaVersion.asJsonLD
      )

      When("The compatibility matrix is updated, TG version changed and TG is restarted")
      updateVersionConfs(serviceVersions.generateOne)
      restartTGWithNewCompatMatrix()

      Then("Re-provisioning is triggered")
      And("The new data can be queried in Jena")

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

    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken
    val initialProjectSchemaVersion = SchemaVersion("8")

    val testEntitiesProject = renkuProjectEntities(visibilityPublic)
      .map(_.copy(version = initialProjectSchemaVersion))
      .withActivities(activityEntities(planEntities()))
      .generateOne
      .copy(members = Set(personEntities.generateOne.copy(maybeGitLabId = user.id.some)))
  }

  private def projectDetailsResponseIsValid(projectDetailsResponse:       ServiceClient.ClientResponse,
                                            expectedProjectSchemaVersion: SchemaVersion
  ) = {
    projectDetailsResponse.status shouldBe Ok
    val Right(projectDetails)       = projectDetailsResponse.jsonBody.as[Json]
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

  private def updateVersionConfs(version: ServiceVersion): Unit =
    Set("./webhook-service",
        "./token-repository",
        "./triples-generator",
        "./event-log",
        "./knowledge-graph",
        "./commit-event-service"
    ) foreach { service =>
      val classesFile     = Paths.get(s"$service/target/scala-2.13/classes/version.conf")
      val testClassesFile = Paths.get(s"$service/target/scala-2.13/test-classes/version.conf")
      if (Files exists testClassesFile)
        Files.writeString(testClassesFile, s"""version = "$version"""")
      else
        Files.writeString(classesFile, s"""version = "$version"""")
    }
}
