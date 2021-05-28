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

package ch.datascience.graph.acceptancetests.flows

import ch.datascience.graph.acceptancetests.data
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices._
import ch.datascience.graph.acceptancetests.tooling.ModelImplicits
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.entities
import ch.datascience.webhookservice.model.HookToken
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should

object RdfStoreProvisioning extends ModelImplicits with Eventually with AcceptanceTestPatience with should.Matchers {

  def `data in the RDF store`[FC <: entities.Project.ForksCount](
      project:            data.Project[FC],
      commitId:           CommitId,
      triples:            JsonLD
  )(implicit accessToken: AccessToken): Assertion = {
    val projectId = project.id

    givenAccessTokenPresentFor(project)

    `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

    `GET <gitlabApi>/projects/:id/repository/commits returning OK with a commit`(projectId, commitId)

    `GET <gitlabApi>/projects/:path returning OK with`(project)

    `GET <triples-generator>/projects/:id/commits/:id returning OK`(project, commitId, triples)

    `GET <gitlabApi>/projects/:path/members returning OK with the list of members`(project)

    webhookServiceClient
      .POST("webhooks/events", HookToken(projectId), data.GitLab.pushEvent(project, commitId))
      .status shouldBe Accepted

    `wait for events to be processed`(projectId)
  }

  def `wait for events to be processed`(projectId: projects.Id): Assertion = eventually {
    val response = eventLogClient.fetchProcessingStatus(projectId)
    response.status                                              shouldBe Ok
    response.bodyAsJson.hcursor.downField("progress").as[Double] shouldBe Right(100d)
  }
}
