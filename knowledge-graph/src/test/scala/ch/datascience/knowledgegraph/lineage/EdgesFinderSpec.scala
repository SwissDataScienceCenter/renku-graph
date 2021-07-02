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
import ch.datascience.generators.CommonGraphGenerators.authUsers
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EdgesFinderSpec extends AnyWordSpec with InMemoryRdfStore with ExternalServiceStubbing with should.Matchers {

  "findEdges" should {

    "return all the edges of the given project " +
      "case when the user is not authenticated and the project is public" in new TestCase {

        val (jsons, exemplarData) = LineageExemplarData(projectEntities[ForksCount.Zero](visibilityPublic).generateOne)

        loadToStore(jsons: _*)

        import exemplarData._

        edgesFinder
          .findEdges(project.path, maybeUser = None)
          .unsafeRunSync() shouldBe Map(
          RunInfo(`activity3 plan1`.toEntityId, RunDate(`activity3 date`.value)) -> (
            Set(`zhbikes folder`.toNodeLocation, `clean_data entity`.toNodeLocation),
            Set(`bikesparquet entity`.toNodeLocation)
          ),
          RunInfo(`activity4 plan2`.toEntityId, RunDate(`activity4 date`.value)) -> (
            Set(`plot_data entity`.toNodeLocation, `bikesparquet entity`.toNodeLocation),
            Set(`grid_plot entity`.toNodeLocation, `cumulative entity`.toNodeLocation)
          )
        )

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
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
        val (jsons, _) = LineageExemplarData(projectEntities[ForksCount.Zero](visibilityNonPublic).generateOne)

        loadToStore(jsons: _*)

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

        val (jsons, exemplarData) = LineageExemplarData(
          projectEntities[ForksCount.Zero](visibilityNonPublic).generateOne.copy(
            members = Set(personEntities.generateOne.copy(maybeGitLabId = Some(authUser.id)))
          )
        )

        loadToStore(jsons: _*)

        import exemplarData._

        edgesFinder
          .findEdges(project.path, Some(authUser))
          .unsafeRunSync() shouldBe Map(
          RunInfo(`activity3 plan1`.toEntityId, RunDate(`activity3 date`.value)) -> (
            Set(`zhbikes folder`.toNodeLocation, `clean_data entity`.toNodeLocation),
            Set(`bikesparquet entity`.toNodeLocation)
          ),
          RunInfo(`activity4 plan2`.toEntityId, RunDate(`activity4 date`.value)) -> (
            Set(`plot_data entity`.toNodeLocation, `bikesparquet entity`.toNodeLocation),
            Set(`grid_plot entity`.toNodeLocation, `cumulative entity`.toNodeLocation)
          )
        )

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
        )
      }
  }

  private trait TestCase {
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val edgesFinder = new EdgesFinderImpl(
      rdfStoreConfig,
      renkuBaseUrl,
      logger,
      new SparqlQueryTimeRecorder(executionTimeRecorder)
    )
  }

  private implicit class NodeDefOps(nodeDef: NodeDef) {
    lazy val toNodeLocation: Node.Location = Node.Location(nodeDef.location)
    lazy val toEntityId:     EntityId      = EntityId.of(nodeDef.location)
  }
}
