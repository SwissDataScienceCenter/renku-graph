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

package io.renku.knowledgegraph.lineage

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import io.renku.jsonld.syntax._
import io.renku.knowledgegraph.lineage.model._
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EdgesFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ExternalServiceStubbing
    with should.Matchers
    with IOSpec {

  "findEdges" should {

    "return all the edges of the given project " +
      "case when the user is not authenticated and the project is public" in new TestCase {

        val exemplarData = LineageExemplarData(projectEntities(visibilityPublic).generateOne)

        import exemplarData._

        loadToStore(project)

        edgesFinder
          .findEdges(project.path, maybeUser = None)
          .unsafeRunSync() shouldBe Map(
          ExecutionInfo(activity1.asEntityId.show, RunDate(activity1.startTime.value)) -> (
            Set(`zhbikes folder`.toNodeLocation, `clean_data entity`.toNodeLocation),
            Set(`bikesparquet entity`.toNodeLocation)
          ),
          ExecutionInfo(activity2.asEntityId.show, RunDate(activity2.startTime.value)) -> (
            Set(`plot_data entity`.toNodeLocation, `bikesparquet entity`.toNodeLocation),
            Set(`grid_plot entity`.toNodeLocation, `cumulative entity`.toNodeLocation)
          ),
          ExecutionInfo(activity3.asEntityId.show, RunDate(activity3.startTime.value)) -> (
            Set(`zhbikes folder`.toNodeLocation, `clean_data entity`.toNodeLocation),
            Set(`bikesparquet entity`.toNodeLocation)
          ),
          ExecutionInfo(activity4.asEntityId.show, RunDate(activity4.startTime.value)) -> (
            Set(`plot_data entity`.toNodeLocation, `bikesparquet entity`.toNodeLocation),
            Set(`grid_plot entity`.toNodeLocation, `cumulative entity`.toNodeLocation)
          )
        )

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    /** in1 in2 in3 in2 \ / \ / plan plan
      * | | out1 out2
      */
    "return all the edges including executions with overridden inputs/outputs" in new TestCase {

      val project = projectEntities(visibilityPublic).generateOne

      val in1  = entityLocations.generateOne
      val in2  = entityLocations.generateOne
      val out1 = entityLocations.generateOne
      val plan = planEntities(CommandInput.fromLocation(in1),
                              CommandInput.fromLocation(in2),
                              CommandOutput.fromLocation(out1)
      ).generateOne

      val activity1 = executionPlanners(fixed(plan), project).generateOne
        .planInputParameterValuesFromChecksum(in1 -> entityChecksums.generateOne, in2 -> entityChecksums.generateOne)
        .buildProvenanceUnsafe()

      val in3  = entityLocations.generateOne
      val out2 = entityLocations.generateOne
      val activity2 = executionPlanners(fixed(plan), project).generateOne
        .planInputParameterOverrides(in1 -> Entity.InputEntity(in3, entityChecksums.generateOne))
        .planInputParameterValuesFromChecksum(in2 -> entityChecksums.generateOne)
        .planOutputParameterOverrides(out1 -> out2)
        .buildProvenanceUnsafe()

      loadToStore(project.addActivities(activity1, activity2))

      edgesFinder
        .findEdges(project.path, maybeUser = None)
        .unsafeRunSync() shouldBe Map(
        ExecutionInfo(activity1.asEntityId.show, RunDate(activity1.startTime.value)) -> (
          Set(Node.Location(in1.value), Node.Location(in2.value)),
          Set(Node.Location(out1.value))
        ),
        ExecutionInfo(activity2.asEntityId.show, RunDate(activity2.startTime.value)) -> (
          Set(Node.Location(in3.value), Node.Location(in2.value)),
          Set(Node.Location(out2.value))
        )
      )
    }

    "return None if there's no lineage for the project " +
      "case when the user is not authenticated and the project is public" in new TestCase {
        edgesFinder
          .findEdges(projectPaths.generateOne, maybeUser = None)
          .unsafeRunSync() shouldBe empty
      }

    "return None if the project is not public " +
      "case when the user is not a member of the project or not authenticated" in new TestCase {
        val exemplarData = LineageExemplarData(projectEntities(visibilityNonPublic).generateOne)

        loadToStore(exemplarData.project)

        edgesFinder
          .findEdges(projectPaths.generateOne, authUsers.generateOption)
          .unsafeRunSync() shouldBe empty

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    "return all the edges of the given project " +
      "case when the user is authenticated and is a member of the project" in new TestCase {
        val authUser = authUsers.generateOne

        val exemplarData = LineageExemplarData(
          projectEntities(visibilityNonPublic).generateOne.copy(
            members = Set(personEntities.generateOne.copy(maybeGitLabId = Some(authUser.id)))
          )
        )
        import exemplarData._

        loadToStore(project)

        edgesFinder
          .findEdges(project.path, Some(authUser))
          .unsafeRunSync() shouldBe Map(
          ExecutionInfo(activity1.asEntityId.show, RunDate(activity1.startTime.value)) -> (
            Set(`zhbikes folder`.toNodeLocation, `clean_data entity`.toNodeLocation),
            Set(`bikesparquet entity`.toNodeLocation)
          ),
          ExecutionInfo(activity2.asEntityId.show, RunDate(activity2.startTime.value)) -> (
            Set(`plot_data entity`.toNodeLocation, `bikesparquet entity`.toNodeLocation),
            Set(`grid_plot entity`.toNodeLocation, `cumulative entity`.toNodeLocation)
          ),
          ExecutionInfo(activity3.asEntityId.show, RunDate(activity3.startTime.value)) -> (
            Set(`zhbikes folder`.toNodeLocation, `clean_data entity`.toNodeLocation),
            Set(`bikesparquet entity`.toNodeLocation)
          ),
          ExecutionInfo(activity4.asEntityId.show, RunDate(activity4.startTime.value)) -> (
            Set(`plot_data entity`.toNodeLocation, `bikesparquet entity`.toNodeLocation),
            Set(`grid_plot entity`.toNodeLocation, `cumulative entity`.toNodeLocation)
          )
        )
      }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val edgesFinder = new EdgesFinderImpl[IO](
      rdfStoreConfig,
      renkuBaseUrl,
      new SparqlQueryTimeRecorder[IO](executionTimeRecorder)
    )
  }

  private implicit class NodeDefOps(nodeDef: NodeDef) {
    lazy val toNodeLocation: Node.Location = Node.Location(nodeDef.location)
  }
}
