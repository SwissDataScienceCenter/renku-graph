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

import LineageGenerators._
import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators.commitIds
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.graphql.lineage.QueryFields.FilePath
import ch.datascience.knowledgegraph.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.graphql.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.RdfStoreData.RDF
import ch.datascience.rdfstore.{InMemoryRdfStore, RdfStoreData}
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class IOLineageFinderSpec extends WordSpec with InMemoryRdfStore with ExternalServiceStubbing {

  "findLineage" should {

    "return the lineage of the given project for a given commit id and file path" in new InMemoryStoreTestCase {
      import testData._

      loadToStore(RDF(triples))

      lineageFinder
        .findLineage(projectPath, commit4Id, FilePath(resultFile1))
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
    }

    "return None if there's no lineage for the project" in new InMemoryStoreTestCase {
      lineageFinder
        .findLineage(projectPath, commitIds.generateOne, filePaths.generateOne)
        .unsafeRunSync() shouldBe None
    }
  }

  private trait InMemoryStoreTestCase {

    val renkuBaseUrl = RdfStoreData.renkuBaseUrl
    val projectPath  = ProjectPath("kuba/zurich-bikes")
    val testData     = RdfStoreData.multiFileAndCommit(projectPath)

    def sourceNode(node: RdfStoreData.MultiFileAndCommitTriples.Resource): SourceNode =
      SourceNode(NodeId(node.name.value), NodeLabel(node.label.value))
    def targetNode(node: RdfStoreData.MultiFileAndCommitTriples.Resource): TargetNode =
      TargetNode(NodeId(node.name.value), NodeLabel(node.label.value))
    def node(node: RdfStoreData.MultiFileAndCommitTriples.Resource): Node = sourceNode(node)

    val lineageFinder = new IOLineageFinder(rdfStoreConfig,
                                            renkuBaseUrl,
                                            TestExecutionTimeRecorder[IO](elapsedTimes.generateOne),
                                            TestLogger())
  }
}
