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

package io.renku.graph.acceptancetests.stubs

import cats.data.NonEmptyList
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.client.{MappingBuilder, WireMock}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.graph.acceptancetests.data
import io.renku.graph.acceptancetests.tooling.{ApplicationServices, TestLogger}
import io.renku.graph.model._
import io.renku.graph.model.events.CommitId
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._

trait RemoteTriplesGenerator {
  self: ApplicationServices =>

  private implicit val graph: GraphClass = GraphClass.Default

  import RemoteTriplesGeneratorWiremockInstance._

  def `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(
      project:             data.Project,
      commitId:            CommitId
  )(implicit gitLabApiUrl: GitLabApiUrl, renkuUrl: RenkuUrl): Unit =
    `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project,
      commitId,
      project.entitiesProject.asJsonLD.flatten.fold(throw _, identity)
    )

  def `GET <triples-generator>/projects/:id/commits/:id returning OK`(
      project:  data.Project,
      commitId: CommitId,
      triples:  JsonLD
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(
          ok(triples.flatten.fold(throw _, _.toJson.spaces2))
        )
    }
    ()
  }

  def mockCommitDataOnTripleGenerator(
      project:   data.Project,
      triples:   JsonLD,
      commitId:  CommitId,
      commitIds: CommitId*
  ): Unit =
    mockCommitDataOnTripleGenerator(project, triples, NonEmptyList(commitId, commitIds.toList))

  def mockCommitDataOnTripleGenerator(
      project:   data.Project,
      triples:   JsonLD,
      commitIds: NonEmptyList[CommitId]
  ): Unit =
    commitIds.toList.foreach { commitId =>
      `GET <triples-generator>/projects/:id/commits/:id returning OK`(project, commitId, triples)
    }

  def `GET <triples-generator>/projects/:id/commits/:id fails non recoverably`(
      project:  data.Project,
      commitId: CommitId
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(badRequest())
    }
    ()
  }

  def `GET <triples-generator>/projects/:id/commits/:id fails recoverably`(
      project:  data.Project,
      commitId: CommitId
  ): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(unauthorized())
    }
    ()
  }

  private def stubFor(mappingBuilder: MappingBuilder): StubMapping = instance.register(mappingBuilder)

  def resetTriplesGenerator(): Unit = server.resetAll()
}

private object RemoteTriplesGeneratorWiremockInstance {
  private val logger = TestLogger()

  val port: Int Refined Positive = 8080

  val instance = WireMock.create().http().host("localhost").port(port.value).build()

  val server = {
    val newServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(port.value))
    newServer.start()
    WireMock.configureFor(newServer.port())
    logger.info(s"Remote Triples Generator stub started")
    newServer
  }

  def shutdownGitLab(): Unit = {
    server.stop()
    server.shutdownServer()
    logger.info(s"Remote Triples Generator stub stopped")
    ()
  }
}
