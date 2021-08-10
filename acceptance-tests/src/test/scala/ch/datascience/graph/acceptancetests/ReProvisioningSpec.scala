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

package ch.datascience.graph.acceptancetests

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.stubs.GitLab.`GET <gitlabApi>/projects/:path returning OK with`
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator.`GET <triples-generator>/projects/:id/commits/:id returning OK`
import ch.datascience.graph.acceptancetests.tooling.GraphServices._
import ch.datascience.graph.acceptancetests.tooling.ResponseTools.ResponseOps
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits, ServiceRun}
import ch.datascience.graph.model
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.projects
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.triplesgenerator
import io.circe.Json
import io.renku.jsonld._
import io.renku.jsonld.syntax.JsonEncoderOps
import org.http4s.Status.Ok
import org.scalactic.source.Position
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.enablers.Retrying
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Minutes, Span}

class ReProvisioningSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with should.Matchers
    with RdfStoreData {

  Feature("ReProvisioning") {

    implicit val accessToken: AccessToken = accessTokens.generateOne
    val initialProjectVersion = SchemaVersion("8")
    val project =
      projects.generateOne.copy(path = model.projects.Path("public/re-provisioning"),
                                visibility = Visibility.Public,
                                version = initialProjectVersion
      )
    val commitId  = commitIds.generateOne
    val committer = persons.generateOne

    Scenario("Update CLI version and expect re-provisioning") {
      Given("The TG is using an older version of the CLI")

      And("There is data from this version in Jena")
      val activity = nonModifiedDataSetActivity(commitId = commitId, committer = committer)(
        projectPath = project.path,
        projectName = project.name,
        projectDateCreated = project.created.date,
        maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
        projectVersion = project.version
      )()

      `data in the RDF store`(project, commitId, committer, JsonLD.arr(activity.asJsonLD))()

      `GET <gitlabApi>/projects/:path returning OK with`(project, maybeCreator = committer.some, withStatistics = true)
      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      projectDetailsResponse.status shouldBe Ok
      val Right(projectDetails) = projectDetailsResponse.bodyAsJson.as[Json]
      val Right(version)        = projectDetails.hcursor.downField("version").as[String]
      version shouldBe initialProjectVersion.value

      val newSchemaVersion = SchemaVersion(nonEmptyStrings().generateOne)
      val newTriples =
        activity
          .copy(committer = activity.committer, project = activity.project.copy(version = newSchemaVersion))
          .asJsonLD

      `GET <triples-generator>/projects/:id/commits/:id returning OK`(project, commitId, JsonLD.arr(newTriples))

      When("The compatibility matrix is updated and the TG is restarted")
      restartTGWithNewCompatMatrix("application-re-provisioning.conf")

      Then("Re-provisioning is triggered")
      And("The new data can be queried in Jena")

      `GET <gitlabApi>/projects/:path returning OK with`(project, maybeCreator = committer.some, withStatistics = true)

      val patience: org.scalatest.concurrent.Eventually.PatienceConfig =
        Eventually.PatienceConfig(timeout = Span(20, Minutes), interval = Span(30000, Millis))

      eventually {
        val updatedProjectDetailsResponse =
          knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

        updatedProjectDetailsResponse.status shouldBe Ok
        val Right(updatedProjectDetails) = updatedProjectDetailsResponse.bodyAsJson.as[Json]
        val Right(updatedVersion)        = updatedProjectDetails.hcursor.downField("version").as[String]
        updatedVersion shouldBe newSchemaVersion.value

      }(patience, Retrying.retryingNatureOfT, Position.here)

    }
  }

  private def restartTGWithNewCompatMatrix(configFilename: String): Unit = {
    val newTriplesGenerator = ServiceRun(
      "triples-generator",
      service = triplesgenerator.Microservice,
      serviceClient = triplesGeneratorClient,
      serviceArgsList = List(() => configFilename)
    )
    stop(newTriplesGenerator.name)
    GraphServices.run(newTriplesGenerator)
  }
}
