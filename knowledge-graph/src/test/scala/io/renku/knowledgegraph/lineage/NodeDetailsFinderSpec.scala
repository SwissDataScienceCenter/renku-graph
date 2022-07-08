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

package io.renku.knowledgegraph.lineage

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.testentities.CommandParameterBase._
import io.renku.graph.model.testentities.{planCommands, _}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.knowledgegraph.lineage.LineageGenerators._
import io.renku.knowledgegraph.lineage.model._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryJenaForSpec, RenkuDataset, SparqlQueryTimeRecorder}
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class NodeDetailsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset
    with ExternalServiceStubbing
    with IOSpec {

  import NodeDetailsFinder._

  "findDetails - locations" should {

    "find details of all entities with the given locations" in new TestCase {

      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne

      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            planEntities(
              CommandInput.fromLocation(input),
              CommandOutput.fromLocation(output)
            ),
            project
          ).map(_.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne).buildProvenanceUnsafe())
        )
        .generateOne

      upload(to = renkuDataset, project)

      val inputEntityNode  = NodeDef(project.activities.head, input).toNode
      val outputEntityNode = NodeDef(project.activities.head, output).toNode

      nodeDetailsFinder
        .findDetails(Set(inputEntityNode.location, outputEntityNode.location), project.path)
        .unsafeRunSync() shouldBe Set(inputEntityNode, outputEntityNode)
    }

    "return no results if no locations given" in new TestCase {
      nodeDetailsFinder
        .findDetails(Set.empty[Node.Location], projectPaths.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }

    "fail if details of the given location cannot be found" in new TestCase {

      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne
      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            planEntities(
              CommandInput.fromLocation(input),
              CommandOutput.fromLocation(output)
            ),
            project
          ).map(
            _.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne).buildProvenanceUnsafe()
          )
        )
        .generateOne

      upload(to = renkuDataset, project)

      val missingLocation = nodeLocations.generateOne

      val exception = intercept[Exception] {
        nodeDetailsFinder
          .findDetails(Set(Node.Location(input.toString), missingLocation), project.path)
          .unsafeRunSync()
      }

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"No entity with $missingLocation"
    }
  }

  "findDetails - activityIds" should {

    import io.renku.generators.jsonld.JsonLDGenerators._

    "find details of all Plans for the given activity ids" in new TestCase {

      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne

      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            planEntities(
              CommandInput.fromLocation(input),
              CommandOutput.fromLocation(output)
            ),
            project
          ).map(
            _.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
              .buildProvenanceUnsafe()
          )
        )
        .addActivity(project =>
          executionPlanners(
            planEntities(CommandInput.fromLocation(output)),
            project
          ).generateOne
            .planInputParameterValuesFromEntity(
              output -> project.activities.head
                .findGenerationEntity(output)
                .getOrElse(throw new Exception(s"No generation entity for $output"))
            )
            .buildProvenanceUnsafe()
        )
        .generateOne

      upload(to = renkuDataset, project)

      val activity1 :: activity2 :: Nil = project.activities

      nodeDetailsFinder
        .findDetails(
          Set(
            ExecutionInfo(activity1.asEntityId.show, activity1.startTime.value),
            ExecutionInfo(activity2.asEntityId.show, activity2.startTime.value)
          ),
          project.path
        )
        .unsafeRunSync() shouldBe Set(NodeDef(activity1).toNode, NodeDef(activity2).toNode)
    }

    "find details of a Plan with mapped command parameters" in new TestCase {
      val input +: output +: errOutput +: Nil =
        entityLocations.generateNonEmptyList(minElements = 3, maxElements = 3).toList

      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            planEntities(
              CommandInput.streamedFromLocation(input),
              CommandOutput.streamedFromLocation(output, CommandOutput.stdOut),
              CommandOutput.streamedFromLocation(errOutput, CommandOutput.stdErr)
            ),
            project
          ).map(
            _.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
              .buildProvenanceUnsafe()
          )
        )
        .generateOne

      upload(to = renkuDataset, project)

      val activity = project.activities.head

      nodeDetailsFinder
        .findDetails(
          Set(ExecutionInfo(activity.asEntityId.show, activity.startTime.value)),
          project.path
        )
        .unsafeRunSync() shouldBe Set(NodeDef(activity).toNode)
    }

    "find details of a Plan with no command" in new TestCase {
      val input +: output +: errOutput +: Nil =
        entityLocations.generateNonEmptyList(minElements = 3, maxElements = 3).toList

      val project: RenkuProject = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            planEntities(
              CommandInput.streamedFromLocation(input),
              CommandOutput.streamedFromLocation(output, CommandOutput.stdOut),
              CommandOutput.streamedFromLocation(errOutput, CommandOutput.stdErr)
            ).andThen(genPlan => genPlan.map(_.copy(maybeCommand = None))),
            project
          ).map(
            _.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne) // add some override values
              .buildProvenanceUnsafe()
          )
        )
        .generateOne

      upload(to = renkuDataset, project)

      val activity = project.activities.head
      val ids = Set(
        ExecutionInfo(activity.asEntityId.show, activity.startTime.value)
      )

      nodeDetailsFinder
        .findDetails( // returns a set of nodes. A node as a location, a label, and set of types
          ids, // execution info has entityId and date
          project.path
        )
        .unsafeRunSync() shouldBe Set(NodeDef(activity).toNode) // a node def is the same as a node
    }

    "return no results if no ids given" in new TestCase {
      nodeDetailsFinder
        .findDetails(Set.empty[ExecutionInfo], projectPaths.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }

    "fail if details of the given location cannot be found" in new TestCase {

      val missingPlan = entityIds.generateOne

      val exception = intercept[Exception] {
        nodeDetailsFinder
          .findDetails(
            Set(ExecutionInfo(missingPlan, timestampsNotInTheFuture.generateOne)),
            projectPaths.generateOne
          )
          .unsafeRunSync()
      }

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"No plan with $missingPlan"
    }
  }

  private trait TestCase {
    implicit val logger:               TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val nodeDetailsFinder = new NodeDetailsFinderImpl[IO](renkuDSConnectionInfo, renkuUrl)
  }

  private implicit class NodeDefOps(nodeDef: NodeDef) {

    lazy val toNode: Node = Node(
      Node.Location(nodeDef.location),
      Node.Label(nodeDef.label),
      nodeDef.types.map(Node.Type.apply)
    )
  }
}
