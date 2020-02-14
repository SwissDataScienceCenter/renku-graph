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

package ch.datascience.knowledgegraph.lineage

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.{filePaths, projectPaths}
import ch.datascience.graph.model.{events, projects}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.bundles.exemplarLineageFlow.NodeDef
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class IOLineageFinderSpec extends WordSpec with InMemoryRdfStore with ExternalServiceStubbing {

  "findLineage" should {

    "return the lineage of the given project for a given commit id and file path" in new InMemoryStoreTestCase {

      val (jsons, examplarData) = exemplarLineageFlow(projectPath)

      loadToStore(jsons: _*)

      import examplarData._

      lineageFinder
        .findLineage(projectPath, commitId, filePath)
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(sourceNode(`sha3 zhbikes`), targetNode(`sha8 renku run`)),
            Edge(sourceNode(`sha7 plot_data`), targetNode(`sha9 renku run`)),
            Edge(sourceNode(`sha7 plot_data`), targetNode(`sha12 step1 renku update`)),
            Edge(sourceNode(`sha7 clean_data`), targetNode(`sha8 renku run`)),
            Edge(sourceNode(`sha7 clean_data`), targetNode(`sha12 step2 renku update`)),
            Edge(sourceNode(`sha8 renku run`), targetNode(`sha8 parquet`)),
            Edge(sourceNode(`sha8 parquet`), targetNode(`sha9 renku run`)),
            Edge(sourceNode(`sha9 renku run`), targetNode(`sha9 plot_data`)),
            Edge(sourceNode(`sha10 zhbikes`), targetNode(`sha12 step2 renku update`)),
            Edge(sourceNode(`sha12 parquet`), targetNode(`sha12 step1 renku update`)),
            Edge(sourceNode(`sha12 step1 renku update`), targetNode(`sha12 step2 grid_plot`)),
            Edge(sourceNode(`sha12 step2 renku update`), targetNode(`sha12 parquet`))
          ),
          nodes = Set(
            node(`sha3 zhbikes`),
            node(`sha7 clean_data`),
            node(`sha7 plot_data`),
            node(`sha8 renku run`),
            node(`sha8 parquet`),
            node(`sha9 renku run`),
            node(`sha9 plot_data`),
            node(`sha10 zhbikes`),
            node(`sha12 step1 renku update`),
            node(`sha12 step2 grid_plot`),
            node(`sha12 step2 renku update`),
            node(`sha12 parquet`)
          )
        )
      )

      logger.loggedOnly(Warn(s"lineage finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "return None if there's no lineage for the project" in new InMemoryStoreTestCase {

      val commitId: events.CommitId   = commitIds.generateOne
      val filePath: projects.FilePath = filePaths.generateOne

      lineageFinder
        .findLineage(projectPath, commitId, filePath)
        .unsafeRunSync() shouldBe None

      logger.loggedOnly(Warn(s"lineage finished${executionTimeRecorder.executionTimeInfo}"))
    }
  }

  private trait InMemoryStoreTestCase {
    val projectPath = projectPaths.generateOne

    def sourceNode(node: NodeDef): SourceNode = SourceNode(
      NodeId(node.name),
      NodeLabel(node.label)
    )

    def targetNode(node: NodeDef): TargetNode = TargetNode(
      NodeId(node.name),
      NodeLabel(node.label)
    )

    def node(node: NodeDef): Node = sourceNode(node)

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val lineageFinder = new IOLineageFinder(
      rdfStoreConfig,
      renkuBaseUrl,
      logger,
      new SparqlQueryTimeRecorder(executionTimeRecorder)
    )
  }
}
