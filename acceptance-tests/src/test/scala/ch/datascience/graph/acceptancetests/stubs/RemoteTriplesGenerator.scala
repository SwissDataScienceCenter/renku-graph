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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.tooling.TestLogger
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.events.CommitId
import ch.datascience.knowledgegraph.projects.model.Project
import ch.datascience.rdfstore.entities.EntitiesGenerators._
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.{Activity, ExecutionPlanner, Person, RunPlan}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.client.{MappingBuilder, WireMock}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.Assertions.fail

object RemoteTriplesGenerator {

  private val logger = TestLogger()
  private val port: Int Refined Positive = 8080

  def `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(
      project:    Project,
      commitId:   CommitId,
      author:     Person,
      cliVersion: CliVersion = currentVersionPair.cliVersion
  ): Unit =
    `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project,
      commitId,
      ExecutionPlanner(
        RunPlan(runPlanNames.generateOne, runPlanCommands.generateOne, commandParameterFactories = Nil),
        (Activity.StartTime(project.created.date.value), author, cliVersion),
        project.toEntitiesProject(cliVersion),
        argumentsValueOverrides = Nil,
        inputsValueOverrides = Nil
      ).buildProvenanceGraph.fold(fail, identity).asJsonLD
    )

  def `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project:  Project,
      commitId: CommitId,
      triples:  JsonLD
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(
          ok(triples.toJson.spaces2)
        )
    }
    ()
  }

  def `GET <triples-generator>/projects/:id/commits/:id fails non recoverably`(
      project:  Project,
      commitId: CommitId
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(badRequest())
    }
    ()
  }

  def `GET <triples-generator>/projects/:id/commits/:id fails recoverably`(
      project:  Project,
      commitId: CommitId
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(unauthorized())
    }
    ()
  }

  private val instance = WireMock.create().http().host("localhost").port(port.value).build()

  private def stubFor(mappingBuilder: MappingBuilder): StubMapping = instance.register(mappingBuilder)

  private val server = {
    val newServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(port.value))
    newServer.start()
    WireMock.configureFor(newServer.port())
    logger.info(s"Remote Triples Generator stub started")
    newServer
  }

  def shutdown(): Unit = {
    server.stop()
    server.shutdownServer()
    logger.info(s"Remote Triples Generator stub stopped")
    ()
  }
}
