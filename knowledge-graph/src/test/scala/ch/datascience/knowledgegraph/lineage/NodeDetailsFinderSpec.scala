/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.testentities.CommandParameterBase._
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class NodeDetailsFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with should.Matchers {

  import NodeDetailsFinder._

  "findDetails - locations" should {

    "find details of all entities with the given locations" in new TestCase {

      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne

      val project = anyProjectEntities
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

      loadToStore(project)

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
      val project = anyProjectEntities
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

      loadToStore(project)

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

  "findDetails - planIds" should {

    import io.renku.jsonld.generators.JsonLDGenerators._

    "find details of all Plans with the given ids" in new TestCase {

      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne

      val project = anyProjectEntities
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

      loadToStore(project)

      val activity1 :: activity2 :: Nil = project.activities

      val activity1Node = NodeDef(activity1).toNode
      val activity2Node = NodeDef(activity2).toNode

      nodeDetailsFinder
        .findDetails(
          Set(
            RunInfo(EntityId.of(activity1Node.location.toString), activity1.startTime.value),
            RunInfo(EntityId.of(activity2Node.location.toString), activity2.startTime.value)
          ),
          project.path
        )
        .unsafeRunSync() shouldBe Set(activity1Node, activity2Node)
    }

    "find details of a Plan with mapped command parameters" in new TestCase {
      val input +: output +: errOutput +: Nil =
        entityLocations.generateNonEmptyList(minElements = 3, maxElements = 3).toList

      val project = anyProjectEntities
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

      loadToStore(project)

      val activity = project.activities.head

      nodeDetailsFinder
        .findDetails(
          Set(RunInfo(EntityId.of(NodeDef(activity).toNode.location.toString), activity.startTime.value)),
          project.path
        )
        .unsafeRunSync() shouldBe Set(NodeDef(activity).toNode)
    }

    "return no results if no ids given" in new TestCase {
      nodeDetailsFinder
        .findDetails(Set.empty[RunInfo], projectPaths.generateOne)
        .unsafeRunSync() shouldBe Set.empty
    }

    "fail if details of the given location cannot be found" in new TestCase {

      val missingPlan = entityIds.generateOne

      val exception = intercept[Exception] {
        nodeDetailsFinder
          .findDetails(
            Set(RunInfo(missingPlan, timestampsNotInTheFuture.generateOne)),
            projectPaths.generateOne
          )
          .unsafeRunSync()
      }

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"No plan with $missingPlan"
    }
  }

  private trait TestCase {
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val nodeDetailsFinder = new NodeDetailsFinderImpl(
      rdfStoreConfig,
      renkuBaseUrl,
      logger,
      new SparqlQueryTimeRecorder(executionTimeRecorder)
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
