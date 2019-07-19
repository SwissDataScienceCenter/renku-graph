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

package ch.datascience.knowledgegraph.graphql.lineage

import TestData._
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.config.RenkuBaseUrl
import ch.datascience.knowledgegraph.graphql.lineage.QueryFields.FilePath
import ch.datascience.knowledgegraph.graphql.lineage.model._
import ch.datascience.knowledgegraph.rdfstore.InMemoryRdfStore
import ch.datascience.knowledgegraph.rdfstore.RDFStoreConfig.FusekiBaseUrl
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOLineageFinderSpec extends WordSpec with InMemoryRdfStore with ExternalServiceStubbing {

  "findLineage" should {

    "return the whole lineage of the given project" in new InMemoryStoreTestCase {
      loadToStore(testData.triples(projectPath))

      lineageFinder
        .findLineage(projectPath, maybeCommitId = None, maybeFilePath = None)
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(sourceNode(`commit1-input-data`), targetNode(`commit3-renku-run`)),
            Edge(sourceNode(`commit2-source-file1`), targetNode(`commit3-renku-run`)),
            Edge(sourceNode(`commit3-renku-run`), targetNode(`commit3-preprocessed-data`)),
            Edge(sourceNode(`commit3-preprocessed-data`), targetNode(`commit4-renku-run`)),
            Edge(sourceNode(`commit2-source-file2`), targetNode(`commit4-renku-run`)),
            Edge(sourceNode(`commit4-renku-run`), targetNode(`commit4-result-file1`)),
            Edge(sourceNode(`commit4-renku-run`), targetNode(`commit4-result-file2`))
          ),
          nodes = Set(
            `commit1-input-data`,
            `commit2-source-file1`,
            `commit2-source-file2`,
            `commit3-renku-run`,
            `commit3-preprocessed-data`,
            `commit4-renku-run`,
            `commit4-result-file1`,
            `commit4-result-file2`
          )
        )
      )
    }

    "return the lineage of the given project for a given commit id" in new InMemoryStoreTestCase {
      loadToStore(testData.triples(projectPath))

      lineageFinder
        .findLineage(projectPath, maybeCommitId = Some(CommitId("0000003")), maybeFilePath = None)
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(sourceNode(`commit1-input-data`), targetNode(`commit3-renku-run`)),
            Edge(sourceNode(`commit2-source-file1`), targetNode(`commit3-renku-run`)),
            Edge(sourceNode(`commit3-renku-run`), targetNode(`commit3-preprocessed-data`)),
            Edge(sourceNode(`commit3-preprocessed-data`), targetNode(`commit4-renku-run`)),
            Edge(sourceNode(`commit4-renku-run`), targetNode(`commit4-result-file1`)),
            Edge(sourceNode(`commit4-renku-run`), targetNode(`commit4-result-file2`))
          ),
          nodes = Set(
            `commit1-input-data`,
            `commit2-source-file1`,
            `commit3-renku-run`,
            `commit3-preprocessed-data`,
            `commit4-renku-run`,
            `commit4-result-file1`,
            `commit4-result-file2`
          )
        )
      )
    }

    "return the lineage of the given project for a given commit id and file path" in new InMemoryStoreTestCase {
      loadToStore(testData.triples(projectPath))

      lineageFinder
        .findLineage(projectPath, Some(CommitId("0000004")), Some(FilePath("result-file-1")))
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(sourceNode(`commit1-input-data`), targetNode(`commit3-renku-run`)),
            Edge(sourceNode(`commit2-source-file1`), targetNode(`commit3-renku-run`)),
            Edge(sourceNode(`commit3-renku-run`), targetNode(`commit3-preprocessed-data`)),
            Edge(sourceNode(`commit3-preprocessed-data`), targetNode(`commit4-renku-run`)),
            Edge(sourceNode(`commit2-source-file2`), targetNode(`commit4-renku-run`)),
            Edge(sourceNode(`commit4-renku-run`), targetNode(`commit4-result-file1`))
          ),
          nodes = Set(
            `commit1-input-data`,
            `commit2-source-file1`,
            `commit2-source-file2`,
            `commit3-renku-run`,
            `commit3-preprocessed-data`,
            `commit4-renku-run`,
            `commit4-result-file1`
          )
        )
      )
    }

    "return None if there's no lineage for the project" in new InMemoryStoreTestCase {
      lineageFinder
        .findLineage(projectPath, maybeCommitId = None, maybeFilePath = None)
        .unsafeRunSync() shouldBe None
    }

    "use Basic Authorization when calling the RDF store" in new MockedTestCase {
      import io.circe.literal._
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/json"))
          .withRequestBody(containing("query="))
          .willReturn(okJson(json"""{"results": {"bindings": []}}""".noSpaces))
      }

      lineageFinder
        .findLineage(projectPath, None, None)
        .unsafeRunSync() shouldBe None
    }

    "fail if remote responds with status different than OK" in new MockedTestCase {
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .willReturn(unauthorized().withBody("some error"))
      }

      intercept[Exception] {
        lineageFinder
          .findLineage(projectPath, None, None)
          .unsafeRunSync()
      }.getMessage shouldBe s"POST ${rdfStoreConfig.fusekiBaseUrl}/${rdfStoreConfig.datasetName}/sparql returned ${Status.Unauthorized}; body: some error"
    }

    "fail if remote responds with unrecognized body" in new MockedTestCase {
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        lineageFinder
          .findLineage(projectPath, None, None)
          .unsafeRunSync()
      }.getMessage should startWith {
        s"POST ${rdfStoreConfig.fusekiBaseUrl}/${rdfStoreConfig.datasetName}/sparql returned ${Status.Ok}; error: "
      }
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait InMemoryStoreTestCase {
    import ch.datascience.generators.CommonGraphGenerators.basicAuthCredentials

    val renkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")
    val projectPath  = ProjectPath("kuba/zurich-bikes")
    val testData     = new TestData(renkuBaseUrl)

    val lineageFinder = new IOLineageFinder(sparqlEndpoint,
                                            basicAuthCredentials.generateOne,
                                            renkuBaseUrl,
                                            TestExecutionTimeRecorder[IO](elapsedTimes.generateOne),
                                            TestLogger())
  }

  private trait MockedTestCase {
    import ch.datascience.graph.model.events.EventsGenerators._
    import ch.datascience.knowledgegraph.GraphServiceGenerators._
    import ch.datascience.knowledgegraph.rdfstore.RDFStoreGenerators._

    val renkuBaseUrl = renkuBaseUrls.generateOne
    val projectPath  = projectPaths.generateOne

    val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(
      fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    )
    val lineageFinder = IOLineageFinder(
      IO.pure(rdfStoreConfig),
      IO.pure(renkuBaseUrl),
      TestLogger()
    ).unsafeRunSync()
  }
}
