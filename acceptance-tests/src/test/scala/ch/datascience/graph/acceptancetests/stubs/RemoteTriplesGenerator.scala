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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.events.CommitId
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import com.github.tomakehurst.wiremock.client.WireMock.{get, ok, stubFor}
import io.renku.jsonld.JsonLD

object RemoteTriplesGenerator {

  def `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(
      project:    Project,
      commitId:   CommitId,
      cliVersion: CliVersion = currentCliVersion
  ): Unit =
    `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project,
      commitId,
      fileCommit(
        commitId = commitId,
        cliVersion = cliVersion
      )(
        projectPath = project.path,
        projectName = project.name,
        projectDateCreated = project.created.date,
        maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
        projectVersion = project.version
      )
    )

  def `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project:  Project,
      commitId: CommitId,
      triples:  JsonLD
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(
          ok(triples.flatten.fold(throw _, identity).toJson.spaces2)
        )
    }
    ()
  }
}
