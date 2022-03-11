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

package io.renku.graph.acceptancetests.knowledgegraph

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, sentenceContaining}
import io.renku.graph.acceptancetests.data.{RdfStoreData, dataProjects}
import io.renku.graph.acceptancetests.flows.RdfStoreProvisioning
import io.renku.graph.acceptancetests.tooling.GraphServices
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{datasetEntities, renkuProjectEntities, visibilityPublic}
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder._
import io.renku.jsonld.syntax._
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters.EntityType
import org.http4s.Status.Ok
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class CrossEntitiesSearchSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with RdfStoreProvisioning
    with RdfStoreData {

  Feature("GET knowledge-graph/entities") {

    val user = authUsers.generateOne
    implicit val accessToken: AccessToken = user.accessToken

    val commonPhrase = nonBlankStrings(minLength = 5).generateOne
    val testEntitiesProject =
      renkuProjectEntities(visibilityPublic)
        .modify(replaceProjectName(sentenceContaining(commonPhrase).generateAs[projects.Name]))
        .modify(
          replaceProjectCreator(
            personEntities(withGitLabId)
              .map(replacePersonName(sentenceContaining(commonPhrase).generateAs[persons.Name]))
              .generateSome
          )
        )
        .withActivities(
          activityEntities(
            planEntities().modify(replacePlanName(sentenceContaining(commonPhrase).generateAs[plans.Name]))
          )
        )
        .withDatasets(
          datasetEntities(provenanceInternal)
            .modify(replaceDSName(sentenceContaining(commonPhrase).generateAs[datasets.Name]))
        )
        .generateOne
    val project = dataProjects(testEntitiesProject).generateOne

    Scenario("As a user I would like to be able to do cross-entity search by calling a REST endpoint") {

      Given("there's relevant data in the RDF Store")
      val commitId = commitIds.generateOne
      mockDataOnGitLabAPIs(project, testEntitiesProject.asJsonLD, commitId)
      `data in the RDF store`(project, commitId)

      When("the user calls the GET knowledge-graph/entities")
      val response = knowledgeGraphClient GET s"knowledge-graph/entities?query=${urlEncode(commonPhrase.value)}"

      Then("he should get OK response with Project, Dataset, Workflow and Person entities")
      response.status shouldBe Ok
      val Right(foundEntitiesInJson) = response.jsonBody.as[List[Json]]
      foundEntitiesInJson
        .map(_.hcursor.downField("type").as[EntityType])
        .sequence
        .fold(fail(_), identity)
        .toSet shouldBe EntityType.all.toSet
    }
  }
}
