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

package ch.datascience.graphservice.graphql.lineage

import TestData._
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.graphservice.config.RenkuBaseUrl
import ch.datascience.graphservice.graphql.lineage.QueryFields.FilePath
import ch.datascience.graphservice.graphql.lineage.model._
import ch.datascience.graphservice.rdfstore.InMemoryRdfStore
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOLineageFinderSpec extends WordSpec with InMemoryRdfStore {

  private val renkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")
  private val projectPath  = ProjectPath("kuba/zurich-bikes")
  private val testData     = new TestData(renkuBaseUrl)

  "findLineage" should {

    "return the whole lineage of the given project" in new TestCase {
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

    "return the lineage of the given project for a given commit id" in new TestCase {
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

    "return the lineage of the given project for a given commit id and file path" in new TestCase {
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

    "return None if there's no lineage for the project" in new TestCase {
      lineageFinder
        .findLineage(projectPath, maybeCommitId = None, maybeFilePath = None)
        .unsafeRunSync() shouldBe None
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val lineageFinder = new IOLineageFinder(sparqlEndpoint,
                                            renkuBaseUrl,
                                            TestExecutionTimeRecorder[IO](elapsedTimes.generateOne),
                                            TestLogger())
  }
}
