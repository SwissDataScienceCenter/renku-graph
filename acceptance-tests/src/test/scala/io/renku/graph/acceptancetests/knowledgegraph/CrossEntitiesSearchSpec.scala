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
import io.circe.Json
import io.renku.entities.search.Criteria.Filters.EntityType
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, sentenceContaining}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model._
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities._
import io.renku.http.client.UrlEncoder._
import org.http4s.Status.Ok
import tooling.{AcceptanceSpec, ApplicationServices}

class CrossEntitiesSearchSpec extends AcceptanceSpec with ApplicationServices with TSProvisioning {

  Feature("GET knowledge-graph/entities") {

    val user = authUsers.generateOne

    val commonPhrase = nonBlankStrings(minLength = 5).generateOne
    val testProject =
      renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
        .modify(replaceProjectName(sentenceContaining(commonPhrase).generateAs[projects.Name]))
        .modify(removeMembers())
        .modify(
          replaceProjectCreator(
            cliShapedPersons
              .map(replacePersonName(sentenceContaining(commonPhrase).generateAs[persons.Name]))
              .generateSome
          )
        )
        .withActivities(
          activityEntities(
            stepPlanEntities(planCommands, cliShapedPersons).map(
              _.replacePlanName(sentenceContaining(commonPhrase).generateAs[plans.Name])
            ),
            authorGen = cliShapedPersons
          )
        )
        .withDatasets(
          datasetEntities(provenanceInternal(cliShapedPersons))
            .modify(replaceDSSlug(sentenceContaining(commonPhrase).generateAs[datasets.Name]))
        )
        .generateOne
    val project = dataProjects(testProject).map(addMemberWithId(user.id, Role.Owner)).generateOne

    Scenario("As a user I would like to be able to do cross-entity search by calling a REST endpoint") {

      Given("there's relevant data in the Triples Store")
      val commitId = commitIds.generateOne
      gitLabStub.setupProject(project, commitId)
      gitLabStub.addAuthenticated(user)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)
      `data in the Triples Store`(project, commitId, user.accessToken)

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
