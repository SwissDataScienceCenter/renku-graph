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

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.generators.Generators.Implicits._
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
import scala.util.Random

class PagedProjectsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  private val pageSize = 50

  "nextProjectsPage" should {

    "return next page of projects for migration each time the method is called" in new TestCase {

      givenMigrationDateFinding(returning = Instant.now().plusSeconds(60).pure[IO])

      val projects = anyProjectEntities
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = projectsDataset, projects: _*)

      val (page1, page2) = projects.map(_.path) splitAt pageSize

      givenMigratedCheck(of = page1, returning = page1.pure[IO])
      givenMigratedCheck(of = page2, returning = page2.pure[IO])

      finder.nextProjectsPage().unsafeRunSync() shouldBe page1
      finder.nextProjectsPage().unsafeRunSync() shouldBe page2
      finder.nextProjectsPage().unsafeRunSync() shouldBe Nil
    }

    "return only projects with creation date before the migration start date" in new TestCase {

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

      givenMigratedCheck(of = List(oldProject.path), returning = List(oldProject.path).pure[IO])

      finder.nextProjectsPage().unsafeRunSync() shouldBe List(oldProject.path)
    }

    "skip projects for which migration events has been already sent" in new TestCase {

      givenMigrationDateFinding(returning = Instant.now().plusSeconds(60).pure[IO])

      val projects = anyProjectEntities
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = projectsDataset, projects: _*)

      val (page1, page2) = projects.map(_.path) splitAt pageSize

      val page1NonMigrated = page1.filterNot(_ == Random.shuffle(page1).head)
      givenMigratedCheck(of = page1, returning = page1NonMigrated.pure[IO])
      val page2NonMigrated = page2.filterNot(_ == Random.shuffle(page2).head)
      givenMigratedCheck(of = page2, returning = page2NonMigrated.pure[IO])

      finder.nextProjectsPage().unsafeRunSync() shouldBe page1NonMigrated
      finder.nextProjectsPage().unsafeRunSync() shouldBe page2NonMigrated
      finder.nextProjectsPage().unsafeRunSync() shouldBe Nil
    }

    "reach for another page if all projects from the found page get filtered out by migrated check" in new TestCase {

      givenMigrationDateFinding(returning = Instant.now().plusSeconds(60).pure[IO])

      val projects = anyProjectEntities
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = projectsDataset, projects: _*)

      val (page1, page2) = projects.map(_.path) splitAt pageSize

      givenMigratedCheck(of = page1, returning = Nil.pure[IO])
      val page2NonMigrated = page2.filterNot(_ == Random.shuffle(page2).head)
      givenMigratedCheck(of = page2, returning = page2NonMigrated.pure[IO])

      finder.nextProjectsPage().unsafeRunSync() shouldBe page2NonMigrated
      finder.nextProjectsPage().unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val migrationDateFinder     = mock[MigrationStartTimeFinder[IO]]
    private val migratedProjectsChecker = mock[MigratedProjectsChecker[IO]]
    val finder = new PagedProjectsFinderImpl[IO](RecordsFinder(projectsDSConnectionInfo),
                                                 migrationDateFinder,
                                                 migratedProjectsChecker,
                                                 Ref.unsafe(1)
    )

    def givenMigrationDateFinding(returning: IO[Instant]) =
      (() => migrationDateFinder.findMigrationStartDate)
        .expects()
        .returning(returning)
        .atLeastOnce()

    def givenMigratedCheck(of: List[projects.Path], returning: IO[List[projects.Path]]) =
      (migratedProjectsChecker.filterNotMigrated _).expects(of).returning(returning)
  }
}