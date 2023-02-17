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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package v10migration

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

import java.time.Instant

class PagedProjectsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  private val chunkSize = 50

  "findProjects" should {

    "find requested page of projects existing in the TS" in new TestCase {

      givenMigrationDateFinding(returning = Instant.now().plusSeconds(60).pure[IO])

      val projects = anyProjectEntities
        .generateList(min = chunkSize + 1, max = Gen.choose(chunkSize + 1, (2 * chunkSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = projectsDataset, projects: _*)

      val (page1, page2) = projects splitAt chunkSize
      finder.findProjects(page = 1).unsafeRunSync() shouldBe page1.map(_.path)
      finder.findProjects(page = 2).unsafeRunSync() shouldBe page2.map(_.path)
      finder.findProjects(page = 3).unsafeRunSync() shouldBe Nil
    }

    "find only projects with creation date before the migration start date" in new TestCase {

      val migrationDate = timestampsNotInTheFuture.generateOne
      givenMigrationDateFinding(returning = migrationDate.pure[IO])

      val beforeMigrationDate = timestamps(max = migrationDate).generateAs[projects.DateCreated]
      val oldProject = anyProjectEntities
        .map(
          _.fold(
            _.copy(dateCreated = beforeMigrationDate),
            _.copy(dateCreated = beforeMigrationDate),
            _.copy(dateCreated = beforeMigrationDate),
            _.copy(dateCreated = beforeMigrationDate)
          )
        )
        .generateOne
        .to[entities.Project]

      val afterMigrationDate = timestampsNotInTheFuture(butYoungerThan = migrationDate).generateAs[projects.DateCreated]
      val newProject = anyProjectEntities
        .map(
          _.fold(
            _.copy(dateCreated = afterMigrationDate),
            _.copy(dateCreated = afterMigrationDate),
            _.copy(dateCreated = afterMigrationDate),
            _.copy(dateCreated = afterMigrationDate)
          )
        )
        .generateOne
        .to[entities.Project]

      upload(to = projectsDataset, oldProject, newProject)

      finder.findProjects(page = 1).unsafeRunSync() shouldBe List(oldProject.path)
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val migrationDateFinder = mock[MigrationDateFinder[IO]]
    val finder = new PagedProjectsFinderImpl[IO](RecordsFinder(projectsDSConnectionInfo), migrationDateFinder)

    def givenMigrationDateFinding(returning: IO[Instant]) =
      (() => migrationDateFinder.findMigrationStartDate)
        .expects()
        .returning(returning)
        .atLeastOnce()
  }
}
