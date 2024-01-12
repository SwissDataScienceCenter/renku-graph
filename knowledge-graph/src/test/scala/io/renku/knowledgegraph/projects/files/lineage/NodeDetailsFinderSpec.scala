/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.files.lineage

import LineageGenerators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.parameterValues.ValueOverride
import io.renku.graph.model.testentities.StepPlanCommandParameter._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.knowledgegraph.KnowledgeGraphJenaSpec
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import model._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class NodeDetailsFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with KnowledgeGraphJenaSpec
    with ExternalServiceStubbing
    with should.Matchers {

  import NodeDetailsFinder._

  "findDetails - locations" should {

    "find details of all entities with the given locations" in projectsDSConfig.use { implicit pcc =>
      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne

      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            stepPlanEntities(
              CommandInput.fromLocation(input),
              CommandOutput.fromLocation(output)
            ),
            project
          ).map(_.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne).buildProvenanceUnsafe())
        )
        .generateOne

      for {
        _ <- uploadToProjects(project)

        inputEntityNode  = NodeDef(project.activities.head, input).toNode
        outputEntityNode = NodeDef(project.activities.head, output).toNode

        _ <- nodeDetailsFinder
               .findDetails(Set(inputEntityNode.location, outputEntityNode.location), project.slug)
               .asserting(_ shouldBe Set(inputEntityNode, outputEntityNode))
      } yield Succeeded
    }

    "return no results if no locations given" in projectsDSConfig.use { implicit pcc =>
      nodeDetailsFinder
        .findDetails(Set.empty[Node.Location], projectSlugs.generateOne)
        .asserting(_ shouldBe Set.empty)
    }

    "fail if details of the given location cannot be found" in projectsDSConfig.use { implicit pcc =>
      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne
      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            stepPlanEntities(
              CommandInput.fromLocation(input),
              CommandOutput.fromLocation(output)
            ),
            project
          ).map(
            _.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne).buildProvenanceUnsafe()
          )
        )
        .generateOne

      for {
        _ <- uploadToProjects(project)

        missingLocation = nodeLocations.generateOne

        _ <- nodeDetailsFinder
               .findDetails(Set(Node.Location(input.toString), missingLocation), project.slug)
               .assertThrowsError[Exception] { exception =>
                 exception            shouldBe an[IllegalArgumentException]
                 exception.getMessage shouldBe s"No entity with $missingLocation"
               }
      } yield Succeeded
    }
  }

  "findDetails - activityIds" should {

    import io.renku.generators.jsonld.JsonLDGenerators._

    "find details of all Plans for the given activity ids" in projectsDSConfig.use { implicit pcc =>
      val input  = entityLocations.generateOne
      val output = entityLocations.generateOne

      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            stepPlanEntities(
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
            stepPlanEntities(CommandInput.fromLocation(output)),
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

      for {
        _ <- uploadToProjects(project)

        activity1 :: activity2 :: Nil = project.activities

        _ <- nodeDetailsFinder
               .findDetails(
                 Set(
                   ExecutionInfo(activity1.asEntityId.show, activity1.startTime.value),
                   ExecutionInfo(activity2.asEntityId.show, activity2.startTime.value)
                 ),
                 project.slug
               )
               .asserting(_ shouldBe Set(NodeDef(activity1).toNode, NodeDef(activity2).toNode))
      } yield Succeeded
    }

    "find details of a Plan with implicit command parameters " +
      "- no implicit params, inputs and outputs to be included in the node's label" in projectsDSConfig.use {
        implicit pcc =>
          val parameter = commandParameterDefaultValueGen.generateOne
          val input     = entityLocations.generateOne
          val output    = entityLocations.generateOne

          val project = anyRenkuProjectEntities
            .addActivity(project =>
              executionPlanners(
                stepPlanEntities(
                  CommandParameter.implicitFrom(parameter),
                  CommandInput.implicitFromLocation(input),
                  CommandOutput.implicitFromLocation(output)
                ),
                project
              ).map(
                _.replaceCommand(planCommands.generateSome)
                  .planParameterValues(parameter -> ValueOverride(parameter.value))
                  .planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
                  .buildProvenanceUnsafe()
              )
            )
            .generateOne

          for {
            _ <- uploadToProjects(project)

            activity :: Nil = project.activities

            _ <- nodeDetailsFinder
                   .findDetails(
                     Set(ExecutionInfo(activity.asEntityId.show, activity.startTime.value)),
                     project.slug
                   )
                   .asserting(_ shouldBe Set(NodeDef(activity).toNode))
          } yield Succeeded
      }

    "find details of a Plan with mapped command parameters" in projectsDSConfig.use { implicit pcc =>
      val input +: output +: errOutput +: Nil =
        entityLocations.generateNonEmptyList(min = 3, max = 3).toList

      val project = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            stepPlanEntities(
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

      for {
        _ <- uploadToProjects(project)

        activity = project.activities.head

        _ <- nodeDetailsFinder
               .findDetails(
                 Set(ExecutionInfo(activity.asEntityId.show, activity.startTime.value)),
                 project.slug
               )
               .asserting(_ shouldBe Set(NodeDef(activity).toNode))
      } yield Succeeded
    }

    "find details of a Plan with no command" in projectsDSConfig.use { implicit pcc =>
      val input +: output +: errOutput +: Nil =
        entityLocations.generateNonEmptyList(min = 3, max = 3).toList

      val project: RenkuProject = anyRenkuProjectEntities
        .addActivity(project =>
          executionPlanners(
            stepPlanEntities(
              CommandInput.streamedFromLocation(input),
              CommandOutput.streamedFromLocation(output, CommandOutput.stdOut),
              CommandOutput.streamedFromLocation(errOutput, CommandOutput.stdErr)
            ).map(_.replaceCommand(to = None)),
            project
          ).map(
            _.planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne) // add some override values
              .buildProvenanceUnsafe()
          )
        )
        .generateOne

      for {
        _ <- uploadToProjects(project)

        activity = project.activities.head
        ids = Set(
                ExecutionInfo(activity.asEntityId.show, activity.startTime.value)
              )

        _ <- nodeDetailsFinder
               .findDetails( // returns a set of nodes. A node as a location, a label, and set of types
                 ids, // execution info has entityId and date
                 project.slug
               )
               .asserting(_ shouldBe Set(NodeDef(activity).toNode)) // a node def is the same as a node
      } yield Succeeded
    }

    "return no results if no ids given" in projectsDSConfig.use { implicit pcc =>
      nodeDetailsFinder
        .findDetails(Set.empty[ExecutionInfo], projectSlugs.generateOne)
        .asserting(_ shouldBe Set.empty)
    }

    "fail if details of the given location cannot be found" in projectsDSConfig.use { implicit pcc =>
      val missingPlan = entityIds.generateOne

      nodeDetailsFinder
        .findDetails(
          Set(ExecutionInfo(missingPlan, timestampsNotInTheFuture.generateOne)),
          projectSlugs.generateOne
        )
        .assertThrowsError[Exception] { exception =>
          exception            shouldBe an[IllegalArgumentException]
          exception.getMessage shouldBe s"No activity with $missingPlan"
        }
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def nodeDetailsFinder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new NodeDetailsFinderImpl[IO](pcc)
  }

  private implicit class NodeDefOps(nodeDef: NodeDef) {

    lazy val toNode: Node = Node(
      Node.Location(nodeDef.location),
      Node.Label(nodeDef.label),
      Node.Type.fromEntityTypes(nodeDef.types).fold(throw _, identity)
    )
  }
}
