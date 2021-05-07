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
import ch.datascience.graph.model.GraphModelGenerators.{authUsers, projectPaths}
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.entities.persons
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import io.renku.jsonld.EntityId
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EdgesFinderSpec extends AnyWordSpec with InMemoryRdfStore with ExternalServiceStubbing with should.Matchers {

  "findEdges" should {

    "return all the edges of the given project " +
      "case when the user is not authenticated and the project is public" in new TestCase {

        val (jsons, exemplarData) =
          exemplarLineageFlow(projectPath, projectVisibility = Gen.oneOf(Some(Visibility.Public), None).generateOne)

        loadToStore(jsons: _*)

        import exemplarData._

        edgesFinder
          .findEdges(projectPath, None)
          .unsafeRunSync() shouldBe Map(
          RunInfo(`sha8 renku run`.toEntityId, RunDate(`sha12 parquet date`)) -> (
            Set(`sha3 zhbikes`.toNodeLocation, `sha7 clean_data`.toNodeLocation),
            Set(`sha8 parquet`.toNodeLocation)
          ),
          RunInfo(`sha9 renku run`.toEntityId, RunDate(`sha12 parquet date`)) -> (
            Set(`sha7 plot_data`.toNodeLocation, `sha8 parquet`.toNodeLocation),
            Set(`sha9 grid_plot`.toNodeLocation, `sha9 cumulative`.toNodeLocation)
          )
        )

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    "return None if there's no lineage for the project " +
      "case when the user is not authenticated and the project is public" in new TestCase {
        edgesFinder
          .findEdges(projectPath, None)
          .unsafeRunSync() shouldBe empty
      }

    "return None if the project is not public " +
      "case when the user is not a member of the project or not authenticated" in new TestCase {
        val (jsons, _) =
          exemplarLineageFlow(
            projectPath,
            projectVisibility = Gen.oneOf(Visibility.Private, Visibility.Internal).generateSome
          )

        loadToStore(jsons: _*)

        edgesFinder
          .findEdges(projectPath, authUsers.generateOption)
          .unsafeRunSync() shouldBe empty

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    "return all the edges of the given project " +
      "case when the user is authenticated and is a member of the project" in new TestCase {
        val authUser = authUsers.generateOne
        val user     = persons.generateOne.copy(maybeGitLabId = Some(authUser.id))

        val (jsons, exemplarData) =
          exemplarLineageFlow(projectPath,
                              projectVisibility = Gen.oneOf(Visibility.Private, Visibility.Internal).generateSome,
                              projectMembers = Set(user)
          )

        loadToStore(jsons: _*)

        import exemplarData._

        edgesFinder
          .findEdges(projectPath, Some(authUser))
          .unsafeRunSync() shouldBe Map(
          RunInfo(`sha8 renku run`.toEntityId, RunDate(`sha12 parquet date`)) -> (
            Set(`sha3 zhbikes`.toNodeLocation, `sha7 clean_data`.toNodeLocation),
            Set(`sha8 parquet`.toNodeLocation)
          ),
          RunInfo(`sha9 renku run`.toEntityId, RunDate(`sha12 parquet date`)) -> (
            Set(`sha7 plot_data`.toNodeLocation, `sha8 parquet`.toNodeLocation),
            Set(`sha9 grid_plot`.toNodeLocation, `sha9 cumulative`.toNodeLocation)
          )
        )

        logger.logged(
          Warn(s"lineage - edges finished${executionTimeRecorder.executionTimeInfo}")
        )
      }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

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
