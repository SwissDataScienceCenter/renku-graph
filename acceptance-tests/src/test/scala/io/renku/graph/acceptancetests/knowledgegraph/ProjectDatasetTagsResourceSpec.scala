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
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data._
import io.renku.graph.acceptancetests.flows.TSProvisioning
import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices}
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.RenkuTinyTypeGenerators.{personEmails, personGitLabIds}
import io.renku.graph.model.projects.Role
import io.renku.graph.model.publicationEvents
import io.renku.graph.model.testentities._
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.Ok
import org.scalatest.EitherValues

class ProjectDatasetTagsResourceSpec
    extends AcceptanceSpec
    with ApplicationServices
    with TSProvisioning
    with EitherValues {

  Feature("GET knowledge-graph/projects/<namespace>/<name>/datasets/:dsName/tags to find project dataset's tags") {

    Scenario("As a user I would like to find project dataset's tags by calling a REST endpoint") {
      val user        = authUsers.generateOne
      val accessToken = user.accessToken

      Given("the user is authenticated")
      gitLabStub.addAuthenticated(user)

      And("there's a project with datasets and tags")
      val (dataset, project) = {
        val creatorGitLabId = personGitLabIds.generateOne
        val creator         = cliShapedPersons.generateOne.copy(maybeEmail = personEmails.generateSome)
        val (ds, testProject) = renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons)
          .modify(removeMembers())
          .addDataset(
            datasetEntities(provenanceInternal(cliShapedPersons))
              .modify(_.replacePublicationEvents(List(publicationEventFactory)))
          )
          .generateOne

        val project = dataProjects(testProject)
          .map(replaceCreatorFrom(creator, creatorGitLabId))
          .map(addMemberFrom(creator, creatorGitLabId, Role.Owner) >>> addMemberWithId(user.id, Role.Maintainer))
          .generateOne

        ds -> project
      }

      val commitId = commitIds.generateOne
      // mockDataOnGitLabAPIs(project, project.entitiesProject.asJsonLD, commitId)
      mockCommitDataOnTripleGenerator(project, toPayloadJsonLD(project), commitId)
      gitLabStub.setupProject(project, commitId)
      `data in the Triples Store`(project, commitId, accessToken)

      When("the user fetches the tags with GET knowledge-graph/projects/:namespace/:name/datasets/:dsName/tags")
      val response = knowledgeGraphClient.GET(
        s"knowledge-graph/projects/${project.slug}/datasets/${dataset.identification.name}/tags",
        accessToken
      )

      Then("he should get OK response with the relevant tags")
      response.status shouldBe Ok
      val tags = response.jsonBody.as[List[Json]].value
      tags
        .map(_.hcursor.downField("name").as[publicationEvents.Name])
        .sequence
        .value shouldBe dataset.publicationEvents.sortBy(_.startDate).reverse.map(_.name)
    }
  }
}
