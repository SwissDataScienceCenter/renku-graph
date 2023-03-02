/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.entities
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Triple
import io.renku.triplesstore.client.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.util.Random

class ProgressFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MigrationsDataset
    with MockFactory {

  "findProgressInfo" should {

    "return info about the done and total number of projects" in new TestCase {

      givenMigrationDateFinding(returning = Instant.now().plusSeconds(60).pure[IO])

      val projects = anyProjectEntities
        .generateList(min = 2)
        .map(_.to[entities.Project])

      upload(to = projectsDataset, projects: _*)(implicitly[EntityFunctions[entities.Project]],
                                                 projectsDSGraphsProducer[entities.Project],
                                                 ioRuntime
      )

      val doneProjects = Random.shuffle(projects).take(positiveInts(projects.size).generateOne.value)
      doneProjects foreach { project =>
        insert(to = migrationsDataset,
               Triple(MigrationToV10.name.asEntityId, renku / "migrated", project.path.asObject)
        )
      }

      finder.findProgressInfo.unsafeRunSync() shouldBe s"${doneProjects.size} of ${projects.size}"
    }
  }

  private trait TestCase {

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val migrationDateFinder = mock[MigrationStartTimeFinder[IO]]

    val finder = new ProgressFinderImpl[IO](migrationDateFinder,
                                            TSClient(migrationsDSConnectionInfo),
                                            TSClient(projectsDSConnectionInfo)
    )

    def givenMigrationDateFinding(returning: IO[Instant]) =
      (() => migrationDateFinder.findMigrationStartDate)
        .expects()
        .returning(returning)
  }
}
