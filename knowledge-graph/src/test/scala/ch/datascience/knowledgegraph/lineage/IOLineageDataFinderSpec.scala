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
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.schemaVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.CommitId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.CommandParameter.{Input, Output}
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.RunPlan.Command
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import eu.timepit.refined.auto._
import io.renku.jsonld.syntax._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class IOLineageDataFinderSpec extends WordSpec with InMemoryRdfStore with ExternalServiceStubbing {

  "findLineage" should {

    "return the lineage of the given project and the file path" in new TestCase {

      val (jsons, exemplarData) = exemplarLineageFlow(projectPath)

      loadToStore(jsons: _*)

      import exemplarData._

      lineageDataFinder
        .find(projectPath)
        .value
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(`sha3 zhbikes`.toNodeLocation, `sha8 renku run`.toNodeLocation),
            Edge(`sha7 plot_data`.toNodeLocation, `sha9 renku run`.toNodeLocation),
            Edge(`sha7 clean_data`.toNodeLocation, `sha8 renku run`.toNodeLocation),
            Edge(`sha8 renku run`.toNodeLocation, `sha8 parquet`.toNodeLocation),
            Edge(`sha8 parquet`.toNodeLocation, `sha9 renku run`.toNodeLocation),
            Edge(`sha9 renku run`.toNodeLocation, `sha9 plot_data`.toNodeLocation),
            Edge(`sha9 renku run`.toNodeLocation, `sha9 cumulative`.toNodeLocation)
          ),
          nodes = Set(
            `sha3 zhbikes`.toNode,
            `sha7 clean_data`.toNode,
            `sha7 plot_data`.toNode,
            `sha8 renku run`.toNode,
            `sha8 parquet`.toNode,
            `sha9 renku run`.toNode,
            `sha9 plot_data`.toNode,
            `sha9 cumulative`.toNode
          )
        )
      )

      logger.logged(
        Warn(s"lineage finished${executionTimeRecorder.executionTimeInfo}"),
        Warn(s"lineage - runPlan details finished${executionTimeRecorder.executionTimeInfo}"),
        Warn(s"lineage - entity details finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "return the lineage of the given project and the file path " +
      "- a case with command parameters without a position" in new TestCase {

      val agent   = generateAgent
      val project = generateProject(projectPath)
      val input   = locations.generateOne
      val inputActivity = Activity(
        CommitId("000007"),
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment                  = "committing 1 file",
        maybeGenerationFactories = List(Generation.factory(entityFactory = Entity.factory(input)))
      )
      val output = locations.generateOne
      val processRunActivity = ProcessRun.standAlone(
        commitIds.generateOne,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment = s"renku run: committing 1 newly added files",
        associationFactory = Association.process(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan.process(
            WorkflowFile.yaml("renku-run.yaml"),
            Command("python"),
            inputs  = List(Input.withoutPositionFrom(inputActivity.entity(input))),
            outputs = List(Output.withoutPositionFactory(activity => Entity(Generation(output, activity))))
          )
        ),
        maybeInformedBy = inputActivity.some
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      val inputEntityNode     = NodeDef(inputActivity.entity(input))
      val processActivityNode = NodeDef(processRunActivity)
      val outputEntityNode    = NodeDef(processRunActivity.processRunAssociation.runPlan.output(output))
      lineageDataFinder
        .find(projectPath)
        .value
        .unsafeRunSync() shouldBe Some(
        Lineage(
          edges = Set(
            Edge(inputEntityNode.toNodeLocation, processActivityNode.toNodeLocation),
            Edge(processActivityNode.toNodeLocation, outputEntityNode.toNodeLocation)
          ),
          nodes = Set(
            inputEntityNode.toNode,
            processActivityNode.toNode,
            outputEntityNode.toNode
          )
        )
      )
    }

    "return None if there's no lineage for the project" in new TestCase {
      lineageDataFinder
        .find(projectPath)
        .value
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val lineageDataFinder = new IOLineageDataFinder(
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
