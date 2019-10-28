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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CommitId, Project}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.rdfstore.triples.{singleFileAndCommit, triples}
import com.github.tomakehurst.wiremock.client.WireMock.{get, ok, stubFor}

object RemoteTriplesGenerator {

  def `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(
      project:       Project,
      commitId:      CommitId,
      schemaVersion: SchemaVersion = currentSchemaVersion
  ): Unit =
    `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project,
      commitId,
      triples(singleFileAndCommit(project.path, commitId = commitId, schemaVersion = schemaVersion)))

  def `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project:  Project,
      commitId: CommitId,
      triples:  JsonLDTriples
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(
          ok(triples.value.spaces2)
        )
    }
    ()
  }
}
