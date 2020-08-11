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

import LineageGenerators._
import cats.effect.IO
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.lineage.model.Node
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
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class NodesDetailsFinderSpec extends WordSpec with InMemoryRdfStore with ExternalServiceStubbing {

  import NodesDetailsFinder._

  "findDetails - locations" should {

    "find details of all entities with the given locations" in new TestCase {

      val input         = locations.generateOne
      val inputActivity = activity(creating = input)
      val output        = locations.generateOne
      val processRunActivity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List(Output.withoutPositionFactory(activity => Entity(Generation(output, activity)))),
        previousActivity = inputActivity
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      val inputEntityNode  = NodeDef(inputActivity.entity(input)).toNode
      val outputEntityNode = NodeDef(processRunActivity.processRunAssociation.runPlan.output(output)).toNode

      nodeDetailsFinder
        .findDetails(Set(inputEntityNode.location, outputEntityNode.location), projectPath)
        .unsafeRunSync() shouldBe Set(inputEntityNode, outputEntityNode)
    }

    "return no results if no locations given" in new TestCase {

      nodeDetailsFinder
        .findDetails(Set.empty[Node.Location], projectPath)
        .unsafeRunSync() shouldBe Set.empty
    }

    "fail if details of the given location cannot be found" in new TestCase {

      val input         = locations.generateOne
      val inputActivity = activity(creating = input)
      val output        = locations.generateOne
      val processRunActivity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List(Output.withoutPositionFactory(activity => Entity(Generation(output, activity)))),
        previousActivity = inputActivity
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      val missingLocation = nodeLocations.generateOne

      val exception = intercept[Exception] {
        nodeDetailsFinder
          .findDetails(Set(Node.Location(input.toString), missingLocation), projectPath)
          .unsafeRunSync()
      }

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"No entity with $missingLocation"
    }
  }

  "findDetails - runPlanIds" should {

    import io.renku.jsonld.generators.JsonLDGenerators._

    "find details of all RunPlans with the given ids" in new TestCase {

      val input         = locations.generateOne
      val inputActivity = activity(creating = input)
      val output        = locations.generateOne
      val processRun1Activity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List(Output.withoutPositionFactory(activity => Entity(Generation(output, activity)))),
        previousActivity = inputActivity
      )
      val processRun2Activity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List.empty,
        previousActivity = processRun1Activity
      )

      loadToStore(inputActivity.asJsonLD, processRun1Activity.asJsonLD, processRun2Activity.asJsonLD)

      val process1ActivityNode = NodeDef(processRun1Activity).toNode
      val process2ActivityNode = NodeDef(processRun2Activity).toNode

      nodeDetailsFinder
        .findDetails(Set(EntityId.of(process1ActivityNode.location.toString),
                         EntityId.of(process2ActivityNode.location.toString)),
                     projectPath)
        .unsafeRunSync() shouldBe Set(process1ActivityNode, process2ActivityNode)
    }

    "find details of a RunPlan with command parameters without a position" in new TestCase {
      val input         = locations.generateOne
      val inputActivity = activity(creating = input)
      val processRunActivity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List(Output.withoutPositionFactory(activity => Entity(Generation(locations.generateOne, activity)))),
        previousActivity = inputActivity
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      nodeDetailsFinder
        .findDetails(Set(EntityId.of(NodeDef(processRunActivity).toNode.location.toString)), projectPath)
        .unsafeRunSync() shouldBe Set(NodeDef(processRunActivity).toNode)
    }

    "find details of a RunPlan with mapped command parameters" in new TestCase {
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

      nodeDetailsFinder
        .findDetails(Set(EntityId.of(NodeDef(processRunActivity).toNode.location.toString)), projectPath)
        .unsafeRunSync() shouldBe Set(NodeDef(processRunActivity).toNode)
    }

    "return no results if no ids given" in new TestCase {

      nodeDetailsFinder
        .findDetails(Set.empty[EntityId], projectPath)
        .unsafeRunSync() shouldBe Set.empty
    }

    "fail if details of the given location cannot be found" in new TestCase {

      val input         = locations.generateOne
      val inputActivity = activity(creating = input)
      val output        = locations.generateOne
      val processRunActivity = processRun(
        inputs           = List(Input.withoutPositionFrom(inputActivity.entity(input))),
        outputs          = List(Output.withoutPositionFactory(activity => Entity(Generation(output, activity)))),
        previousActivity = inputActivity
      )

      loadToStore(inputActivity.asJsonLD, processRunActivity.asJsonLD)

      val missingRunPlan = entityIds.generateOne

      val exception = intercept[Exception] {
        nodeDetailsFinder
          .findDetails(Set(EntityId.of(NodeDef(processRunActivity).toNode.location.toString), missingRunPlan),
                       projectPath)
          .unsafeRunSync()
      }

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"No runPlan with $missingRunPlan"
    }
  }

  private trait TestCase {
    val projectPath     = projectPaths.generateOne
    private val agent   = generateAgent
    private val project = generateProject(projectPath)

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val nodeDetailsFinder = new IONodesDetailsFinder(
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
          agent.copy(cliVersion = cliVersions.generateOne),
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

    lazy val toNode: Node = Node(
      Node.Location(nodeDef.location),
      Node.Label(nodeDef.label),
      nodeDef.types.map(Node.Type.apply)
    )
  }
}
