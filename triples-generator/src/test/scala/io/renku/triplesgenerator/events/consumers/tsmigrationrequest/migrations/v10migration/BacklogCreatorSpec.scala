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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package v10migration

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.graph.model.versions.SchemaVersion
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import tooling._

import java.time.Instant

class BacklogCreatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with should.Matchers
    with AsyncMockFactory {

  private val pageSize = 50
  private val v9       = SchemaVersion("9")
  private val v10      = SchemaVersion("10")

  "createBacklog" should {

    "find all projects that are either in schema v9 or have no schema but date created before migration start date " +
      "and copy their slugs into the migrations DS" in allDSConfigs.use {
        case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
          givenMigrationDateFinding(returning = Instant.now().pure[IO])

          val projects = anyRenkuProjectEntities
            .map(setSchema(v9))
            .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)

          for {
            _ <- fetchBacklogProjects.asserting(_ shouldBe Nil)

            _ <- uploadToProjects(projects: _*)

            _ <- backlogCreator.createBacklog().assertNoException

            _ <- fetchBacklogProjects.asserting(_.toSet shouldBe projects.map(_.slug).toSet)
          } yield Succeeded
      }

    "skip projects in schema different than v9" in allDSConfigs.use {
      case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
        val migrationDate = timestampsNotInTheFuture.generateOne
        givenMigrationDateFinding(returning = migrationDate.pure[IO])

        val v9Project1 = {
          val dateCreated = timestamps(max = migrationDate).generateAs(projects.DateCreated)
          anyRenkuProjectEntities
            .modify(replaceProjectDateCreated(dateCreated))
            .modify(replaceProjectDateModified(projectModifiedDates(dateCreated.value).generateOne))
            .map(setSchema(v9))
            .generateOne
        }
        val v10Project = anyRenkuProjectEntities.map(setSchema(v10)).generateOne
        val v9Project2 = {
          val dateCreated = timestampsNotInTheFuture(butYoungerThan = migrationDate).generateAs(projects.DateCreated)
          anyRenkuProjectEntities
            .modify(replaceProjectDateCreated(dateCreated))
            .modify(replaceProjectDateModified(projectModifiedDates(dateCreated.value).generateOne))
            .map(setSchema(v9))
            .generateOne
        }

        for {
          _ <- fetchBacklogProjects.asserting(_ shouldBe Nil)

          _ <- uploadToProjects(v9Project1, v10Project, v9Project2)

          _ <- backlogCreator.createBacklog().assertNoException

          _ <- fetchBacklogProjects.asserting(_.toSet shouldBe Set(v9Project1.slug, v9Project2.slug))
        } yield Succeeded
    }

    "skip projects without schema but dateCreated before the migration start time" in allDSConfigs.use {
      case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
        val migrationDate = timestampsNotInTheFuture.generateOne
        givenMigrationDateFinding(returning = migrationDate.pure[IO])

        val oldProject1 = anyNonRenkuProjectEntities
          .modify(replaceProjectDateCreated(timestamps(max = migrationDate).generateAs(projects.DateCreated)))
          .generateOne
        val newProject = anyNonRenkuProjectEntities
          .modify(
            replaceProjectDateCreated(
              timestampsNotInTheFuture(butYoungerThan = migrationDate).generateAs(projects.DateCreated)
            )
          )
          .generateOne
        val oldProject2 = anyNonRenkuProjectEntities
          .modify(replaceProjectDateCreated(timestamps(max = migrationDate).generateAs(projects.DateCreated)))
          .generateOne

        for {
          _ <- fetchBacklogProjects.asserting(_ shouldBe Nil)

          _ <- uploadToProjects(oldProject1, newProject, oldProject2)

          _ <- backlogCreator.createBacklog().assertNoException

          _ <- fetchBacklogProjects.asserting(_.toSet shouldBe Set(oldProject1.slug, oldProject2.slug))
        } yield Succeeded
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private val migrationDateFinder = mock[MigrationStartTimeFinder[IO]]
  private def backlogCreator(implicit pcc: ProjectsConnectionConfig, mcc: MigrationsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new BacklogCreatorImpl[IO](migrationDateFinder, RecordsFinder[IO](pcc), TSClient[IO](mcc))
  }

  private def givenMigrationDateFinding(returning: IO[Instant]) =
    (() => migrationDateFinder.findMigrationStartDate)
      .expects()
      .returning(returning)
      .atLeastOnce()

  private def fetchBacklogProjects(implicit mcc: MigrationsConnectionConfig) =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test V10 backlog",
        Prefixes of renku -> "renku",
        s"""|SELECT ?slug
            |WHERE {
            |  ${MigrationToV10.name.asEntityId.asSparql.sparql} renku:toBeMigrated ?slug
            |}
            |""".stripMargin
      )
    ).map(_.flatMap(_.get("slug").map(projects.Slug)))

  private def setSchema(version: SchemaVersion): Project => Project =
    _.fold(_.copy(version = version), _.copy(version = version), identity, identity)
}
