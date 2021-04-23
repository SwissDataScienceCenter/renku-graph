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
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.cliVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory
import ch.datascience.rdfstore.entities.CommandParameter.Output.OutputFactory
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
import ch.datascience.rdfstore.entities.RunPlan.Command
import ch.datascience.rdfstore.entities._
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EdgesFinderSpec extends AnyWordSpec with InMemoryRdfStore with ExternalServiceStubbing with should.Matchers {

  "findEdges" should {

    "return all the edges of the given project" in new TestCase {

      val (jsons, exemplarData) = exemplarLineageFlow(projectPath)

      loadToStore(jsons: _*)

      import exemplarData._

      edgesFinder
        .findEdges(projectPath)
        .unsafeRunSync() shouldBe Map(
        RunInfo(`sha8 renku run`.toEntityId, `sha8 renku run date`) -> (
          Set(`sha3 zhbikes`.toNodeLocation, `sha7 clean_data`.toNodeLocation),
          Set(`sha8 parquet`.toNodeLocation)
        ),
        RunInfo(`sha9 renku run`.toEntityId, `sha9 renku run date`) -> (
          Set(`sha7 plot_data`.toNodeLocation, `sha8 parquet`.toNodeLocation),
          Set(`sha9 grid_plot`.toNodeLocation, `sha9 cumulative`.toNodeLocation)
        )
      )

      logger.logged(
        Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "return None if there's no lineage for the project" in new TestCase {
      edgesFinder
        .findEdges(projectPath)
        .unsafeRunSync() shouldBe empty
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne
    val agent       = generateAgent
    val project     = generateProject(projectPath)

    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val edgesFinder = new EdgesFinderImpl(
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
      comment = "committing 1 file",
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
            inputs = inputs,
            outputs = outputs
          )
        ),
        maybeInformedBy = previousActivity.some
      )
  }

  private implicit class NodeDefOps(nodeDef: NodeDef) {
    lazy val toNodeLocation: Node.Location = Node.Location(nodeDef.location)
    lazy val toEntityId:     EntityId      = EntityId.of(nodeDef.location)
  }
}
