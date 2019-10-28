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

package ch.datascience.knowledgegraph.lineage

import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.model.{events, projects}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.tinytypes.TinyTypeConverter
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class IOLineageFinderSpec extends WordSpec with InMemoryRdfStore with ExternalServiceStubbing {

  "findLineage" should {

    "return the lineage of the given project for a given commit id and file path" in new InMemoryStoreTestCase {

      loadToStore(triples(multiFileAndCommit(projectPath, data = multiFileAndCommitData)))

      import multiFileAndCommitData._
      lineageFinder
        .findLineage(projectPath, commit4Id, resultFile1)
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
            node(`commit1-input-data`),
            node(`commit2-source-file1`),
            node(`commit2-source-file2`),
            node(`commit3-renku-run`),
            node(`commit3-preprocessed-data`),
            node(`commit4-renku-run`),
            node(`commit4-result-file1`)
          )
        )
      )

      logger.loggedOnly(
        Warn(
          s"Searched for lineage for $projectPath commit: $commit4Id filePath: $resultFile1${executionTimeRecorder.executionTimeInfo}"
        )
      )
    }

    "return None if there's no lineage for the project" in new InMemoryStoreTestCase {
      val commitId: events.CommitId   = commitIds.generateOne
      val filePath: projects.FilePath = filePaths.generateOne

      lineageFinder
        .findLineage(projectPath, commitId, filePath)
        .unsafeRunSync() shouldBe None

      logger.loggedOnly(
        Warn(
          s"Searched for lineage for $projectPath commit: $commitId filePath: $filePath${executionTimeRecorder.executionTimeInfo}"
        )
      )
    }
  }

  private trait InMemoryStoreTestCase {

    val projectPath            = ProjectPath("namespace/test-lineage")
    val multiFileAndCommitData = multiFileAndCommit.MultiFileAndCommitData()
    import multiFileAndCommit._

    def sourceNode(node: Resource): SourceNode = SourceNode(
      node.name.as[Try, NodeId].fold(throw _, identity),
      NodeLabel(node.label.value)
    )
    def targetNode(node: Resource): TargetNode = TargetNode(
      node.name.as[Try, NodeId].fold(throw _, identity),
      NodeLabel(node.label.value)
    )
    def node(node: Resource): Node = sourceNode(node)

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val lineageFinder = new IOLineageFinder(
      rdfStoreConfig,
      renkuBaseUrl,
      executionTimeRecorder,
      logger
    )

    private implicit val resourceNameToNodeId: TinyTypeConverter[ResourceName, NodeId] = { name =>
      NodeId.from(name.value.replace(fusekiBaseUrl.value, ""))
    }
  }
}
