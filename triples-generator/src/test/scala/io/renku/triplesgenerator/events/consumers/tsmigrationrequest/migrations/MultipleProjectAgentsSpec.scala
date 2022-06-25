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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.cliVersions
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class MultipleProjectAgentsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryRdfStore
    with MockFactory {

  "run" should {

    "find projects with multiple schema:agent and remove the additional ones" in new TestCase {
      satisfyMocks

      val correctProject = renkuProjectEntities(anyVisibility).generateOne.to[entities.RenkuProject.WithoutParent]
      val brokenProject  = renkuProjectEntities(anyVisibility).generateOne.to[entities.RenkuProject.WithoutParent]

      loadToStore(correctProject, brokenProject)

      val otherAgent = cliVersions.generateOne
      insertTriple(brokenProject.resourceId, "schema:agent", show"'$otherAgent'")

      findAgents(brokenProject.resourceId) shouldBe Set(brokenProject.agent, otherAgent)

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      findAgents(brokenProject.resourceId)  shouldBe List(brokenProject.agent, otherAgent).sorted.lastOption.toSet
      findAgents(correctProject.resourceId) shouldBe Set(correctProject.agent)
    }
  }

  "apply" should {
    "return an RegisteredMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  private trait TestCase {
    implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recordsFinder     = RecordsFinder[IO](renkuStoreConfig)
    val updateRunner      = UpdateQueryRunner[IO](renkuStoreConfig)
    val migration         = new MultipleProjectAgents[IO](executionRegister, recordsFinder, updateRunner)

    lazy val satisfyMocks = {
      (executionRegister.findExecution _).expects(migration.name).returning(Option.empty[ServiceVersion].pure[IO])
      (executionRegister.registerExecution _).expects(migration.name).returning(().pure[IO])
    }
  }

  private def findAgents(id: projects.ResourceId): Set[CliVersion] =
    runQuery(s"""|SELECT ?agent 
                 |WHERE { 
                 |  ${id.showAs[RdfResource]} a schema:Project;
                 |                            schema:agent ?agent
                 |}""".stripMargin)
      .unsafeRunSync()
      .map(row => CliVersion(row("agent")))
      .toSet
}
