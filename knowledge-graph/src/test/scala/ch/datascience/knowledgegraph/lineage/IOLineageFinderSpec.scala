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
import ch.datascience.graph.model.GraphModelGenerators.{filePaths, projectPaths}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
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

    "return the lineage of the given project for a given commit id and file path" in new TestCase {

      val (jsons, examplarData) = exemplarLineageFlow(projectPath)

      loadToStore(jsons: _*)

      import examplarData._

      lineageFinder
        .findLineage(projectPath, filePath)
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(`sha3 zhbikes`.toNodeLocation, `sha8 renku run`.toNodeLocation),
            Edge(`sha7 plot_data`.toNodeLocation, `sha9 renku run`.toNodeLocation),
            Edge(`sha7 clean_data`.toNodeLocation, `sha8 renku run`.toNodeLocation),
            Edge(`sha8 renku run`.toNodeLocation, `sha8 parquet`.toNodeLocation),
            Edge(`sha8 parquet`.toNodeLocation, `sha9 renku run`.toNodeLocation),
            Edge(`sha9 renku run`.toNodeLocation, `sha9 plot_data`.toNodeLocation)
          ),
          nodes = Set(
            `sha3 zhbikes`.toNode,
            `sha7 clean_data`.toNode,
            `sha7 plot_data`.toNode,
            `sha8 renku run`.toNode,
            `sha8 parquet`.toNode,
            `sha9 renku run`.toNode,
            `sha9 plot_data`.toNode
          )
        )
      )

      logger.logged(
        Warn(s"lineage finished${executionTimeRecorder.executionTimeInfo}"),
        Warn(s"lineage - node details finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "return None if there's no lineage for the project" in new TestCase {
      lineageFinder
        .findLineage(projectPath, filePaths.generateOne)
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val lineageFinder = new IOLineageFinder(
      rdfStoreConfig,
      renkuBaseUrl,
      logger,
      new SparqlQueryTimeRecorder(executionTimeRecorder)
    )
  }

  private implicit class NodeDefOps(nodeDef: NodeDef) {
    lazy val toNodeLocation: Node.Location = Node.Location(nodeDef.location)
    lazy val toNode: Node = Node(
      toNodeLocation,
      Node.Label(nodeDef.label),
      nodeDef.types.map(Node.Type.apply)
    )
  }
}
