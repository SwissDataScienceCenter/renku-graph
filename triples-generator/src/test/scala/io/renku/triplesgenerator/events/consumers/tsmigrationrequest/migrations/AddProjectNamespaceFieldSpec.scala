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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.config.ServiceVersion
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{entities, projects}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class AddProjectNamespaceFieldSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset
    with MockFactory {

  "run" should {

    "find all schema:Project entities and insert renku:projectNamespace to them" in new TestCase {
      satisfyMocks

      val projects             = anyProjectEntities.generateNonEmptyList().toList.map(_.to[entities.Project])
      val projectWithNamespace = anyProjectEntities.generateOne.to[entities.Project]

      upload(to = renkuDataset, projectWithNamespace :: projects: _*)

      projects foreach { project =>
        delete(from = renkuDataset,
               Triple(project.resourceId, renku / "projectNamespace", project.path.toNamespace.show)
        )

        findNamespace(project) shouldBe None
      }

      findNamespace(projectWithNamespace) shouldBe Some(projectWithNamespace.path.toNamespace)

      migration.run().value.unsafeRunSync() shouldBe ().asRight

      projectWithNamespace :: projects foreach { project =>
        findNamespace(project) shouldBe Some(project.path.toNamespace)
      }
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
    val recordsFinder     = RecordsFinder[IO](renkuDSConnectionInfo)
    val updateRunner      = UpdateQueryRunner[IO](renkuDSConnectionInfo)
    val migration         = new AddProjectNamespaceField[IO](executionRegister, recordsFinder, updateRunner)

    lazy val satisfyMocks = {
      (executionRegister.findExecution _).expects(migration.name).returning(Option.empty[ServiceVersion].pure[IO])
      (executionRegister.registerExecution _).expects(migration.name).returning(().pure[IO])
    }
  }

  private def findNamespace(project: entities.Project): Option[projects.Namespace] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch project namespace",
        Prefixes of renku -> "renku",
        s"""|SELECT ?namespace
            |WHERE { ${project.resourceId.showAs[RdfResource]} renku:projectNamespace ?namespace }""".stripMargin
      )
    )
      .unsafeRunSync()
      .flatMap(_.get("namespace").map(projects.Namespace)) match {
      case Nil      => None
      case n :: Nil => Some(n)
      case many     => fail(s"Multiple renku:projectNamespace props on ${project.path} -> ${many.mkString(", ")}")
    }
}
