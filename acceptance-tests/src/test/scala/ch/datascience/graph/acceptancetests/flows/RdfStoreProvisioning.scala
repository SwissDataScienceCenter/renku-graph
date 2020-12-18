/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.flows.AccessTokenPresence._
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices._
import ch.datascience.graph.acceptancetests.tooling.{ModelImplicits, RDFStore}
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.events.{CommitId, EventStatus}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.users.Email
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.webhookservice.model.HookToken
import io.renku.jsonld.JsonLD
import org.http4s.Status._
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should

object RdfStoreProvisioning extends ModelImplicits with Eventually with AcceptanceTestPatience with should.Matchers {

  def `data in the RDF store`(
      project:            Project,
      commitId:           CommitId,
      committer:          Person,
      cliVersion:         CliVersion = currentVersionPair.cliVersion
  )(implicit accessToken: AccessToken): Assertion =
    `data in the RDF store`(
      project,
      commitId,
      committer,
      fileCommit(
        commitId = commitId,
        committer = committer,
        cliVersion = cliVersion
      )(projectPath = project.path, projectVersion = project.version)
    )

  def `data in the RDF store`(
      project:            Project,
      commitId:           CommitId,
      committer:          Person,
      triples:            JsonLD
  )(implicit accessToken: AccessToken): Assertion = {
    val projectId = project.id

    givenAccessTokenPresentFor(project)

    `GET <gitlabApi>/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

    `GET <gitlabApi>/projects/:path returning OK with`(project, maybeCreator = Some(committer))

    `GET <triples-generator>/projects/:id/commits/:id returning OK`(project, commitId, triples)

    `GET <gitlabApi>/projects/:path/members returning OK with the list of members`(project.path, committer.asMember())

    webhookServiceClient
      .POST("webhooks/events", HookToken(projectId), data.GitLab.pushEvent(project, commitId))
      .status shouldBe Accepted

    eventually {
      EventLog.findEvents(projectId, status = New, TriplesStore) shouldBe List(commitId)
    }

    eventually {
      RDFStore
        .run(
          s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
              |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
              |
              |SELECT ?label
              |WHERE {
              |  ?id rdf:type <http://www.w3.org/ns/prov#Activity>;
              |      rdfs:label ?label.
              |  FILTER (CONTAINS (?label, "$commitId"))    
              |}
              |""".stripMargin
        )
        .exists(_.get("label").exists(_ contains commitId.value)) shouldBe true
    }

    eventually {
      EventLog.findEvents(projectId, status = EventStatus.GeneratingTriples).isEmpty shouldBe true
    }
  }

  def `triples updates run`(emails: Set[Email]): Assertion = eventually {

    val emailsInStore = RDFStore
      .run("""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
              |PREFIX schema: <http://schema.org/>
              |
              |SELECT ?email
              |WHERE {
              |  ?resource rdf:type <http://schema.org/Person> ;
              |            schema:email ?email .
              |}
              |""".stripMargin)
      .flatMap(_.get("email"))
      .toSet

    (emails.map(_.value) diff emailsInStore).isEmpty shouldBe true
  }
}
