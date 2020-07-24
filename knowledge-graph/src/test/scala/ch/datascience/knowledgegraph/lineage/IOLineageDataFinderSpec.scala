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
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory
import ch.datascience.rdfstore.entities.CommandParameter.Mapping.IOStream._
import ch.datascience.rdfstore.entities.CommandParameter.Output.OutputFactory
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
        .unsafeRunSync() shouldBe Lineage(
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
      ).some

      logger.logged(
        Warn(s"lineage finished${executionTimeRecorder.executionTimeInfo}"),
        Warn(s"lineage - runPlan details finished${executionTimeRecorder.executionTimeInfo}"),
        Warn(s"lineage - entity details finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "return the lineage of the given project and the file path " +
      "- a case with command parameters without a position" in new TestCase {

      val input         = locations.generateOne
      val inputActivity = activity(creating = input)
      val output        = locations.generateOne
      val processRunActivity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List(Output.withoutPositionFactory(activity => Entity(Generation(output, activity)))),
        previousActivity = inputActivity
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      val inputEntityNode     = NodeDef(inputActivity.entity(input))
      val processActivityNode = NodeDef(processRunActivity)
      val outputEntityNode    = NodeDef(processRunActivity.processRunAssociation.runPlan.output(output))
      lineageDataFinder
        .find(projectPath)
        .value
        .unsafeRunSync() shouldBe Lineage(
        edges = Set(
          Edge(inputEntityNode.toNodeLocation, processActivityNode.toNodeLocation),
          Edge(processActivityNode.toNodeLocation, outputEntityNode.toNodeLocation)
        ),
        nodes = Set(
          inputEntityNode.toNode,
          processActivityNode.toNode,
          outputEntityNode.toNode
        )
      ).some
    }

    "return the lineage of the given project and the file path " +
      "- a case with mapped command parameters" in new TestCase {

      val input +: output +: errOutput +: Nil = locations.generateNonEmptyList(minElements = 3, maxElements = 3).toList
      val inputActivity                       = activity(creating = input)
      val processRunActivity = processRun(
        inputs = List(Input.streamFrom(inputActivity.entity(input))),
        outputs = List(
          Output.streamFactory(activity => Entity(Generation(output, activity)), to    = StdOut),
          Output.streamFactory(activity => Entity(Generation(errOutput, activity)), to = StdErr)
        ),
        previousActivity = inputActivity
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      val inputEntityNode     = NodeDef(inputActivity.entity(input))
      val processActivityNode = NodeDef(processRunActivity)
      val outputEntityNode    = NodeDef(processRunActivity.processRunAssociation.runPlan.output(output))
      val errOutputEntityNode = NodeDef(processRunActivity.processRunAssociation.runPlan.output(errOutput))

      lineageDataFinder
        .find(projectPath)
        .value
        .unsafeRunSync() shouldBe Lineage(
        edges = Set(
          Edge(inputEntityNode.toNodeLocation, processActivityNode.toNodeLocation),
          Edge(processActivityNode.toNodeLocation, outputEntityNode.toNodeLocation),
          Edge(processActivityNode.toNodeLocation, errOutputEntityNode.toNodeLocation)
        ),
        nodes = Set(
          inputEntityNode.toNode,
          processActivityNode.toNode,
          outputEntityNode.toNode,
          errOutputEntityNode.toNode
        )
      ).some
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
    val agent       = generateAgent
    val project     = generateProject(projectPath)

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val lineageDataFinder = new IOLineageDataFinder(
      rdfStoreConfig,
      renkuBaseUrl,
      logger,
      new SparqlQueryTimeRecorder(executionTimeRecorder)
    )

    def activity(creating: Location) = Activity(
      commitIds.generateOne,
      committedDates.generateOne,
      committer = persons.generateOne,
      project,
      agent,
      comment                  = "committing 1 file",
      maybeGenerationFactories = List(Generation.factory(entityFactory = Entity.factory(creating)))
    )

    def processRun(inputs: List[InputFactory], outputs: List[OutputFactory], previousActivity: Activity) =
      ProcessRun.standAlone(
        commitIds.generateOne,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment = s"renku run: committing 1 newly added files",
        associationFactory = Association.process(
          agent.copy(schemaVersion = schemaVersions.generateOne),
          RunPlan.process(
            WorkflowFile.yaml("renku-run.yaml"),
            Command("cat"),
            inputs  = inputs,
            outputs = outputs
          )
        ),
        maybeInformedBy = previousActivity.some
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
