/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.graph.acceptancetests.db.EventLog
import ch.datascience.graph.acceptancetests.model
import ch.datascience.graph.acceptancetests.stubs.GitLab._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.tooling.GraphServices._
import ch.datascience.graph.acceptancetests.tooling.RDFStore
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CommitId, Project}
import ch.datascience.webhookservice.model.HookToken
import org.http4s.Status._
import org.scalatest.Assertion
import org.scalatest.Matchers._
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

object RdfStoreProvisioning extends Eventually with IntegrationPatience {

  def `data in the RDF store`(project: Project, commitId: CommitId, schemaVersion: SchemaVersion): Assertion = {
    val projectId = project.id

    `GET <gitlab>/api/v4/projects/:id/repository/commits/:sha returning OK with some event`(projectId, commitId)

    `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId, schemaVersion)

    webhookServiceClient
      .POST("webhooks/events", HookToken(projectId), model.GitLab.pushEvent(project, commitId))
      .status shouldBe Accepted

    eventually {
      EventLog.findEvents(projectId, status = New) shouldBe List(commitId)
    }

    eventually {
      RDFStore.findAllTriplesNumber() should be > 0
    }
  }
}
