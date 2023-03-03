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
import eu.timepit.refined.auto._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.testentities._
import io.renku.graph.model.versions.SchemaVersion
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

import java.time.Instant

class BacklogCreatorSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MigrationsDataset
    with MockFactory {

  private val pageSize = 50
  private val v9       = SchemaVersion("9")
  private val v10      = SchemaVersion("10")

  "createBacklog" should {

    "find all projects that are either in schema v9 or have no schema but date created before migration start date " +
      "and copy their paths into the migrations DS" in new TestCase {

        givenMigrationDateFinding(returning = Instant.now().pure[IO])

        val projects = anyRenkuProjectEntities
          .map(setSchema(v9))
          .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)

        fetchBacklogProjects shouldBe Nil

        upload(to = projectsDataset, projects: _*)(implicitly[EntityFunctions[Project]],
                                                   projectsDSGraphsProducer[Project],
                                                   ioRuntime
        )

        backlogCreator.createBacklog().unsafeRunSync() shouldBe ()

        fetchBacklogProjects.toSet shouldBe projects.map(_.path).toSet
      }

    "skip projects in schema different than v9" in new TestCase {

      val migrationDate = timestampsNotInTheFuture.generateOne
      givenMigrationDateFinding(returning = migrationDate.pure[IO])

      val v9Project1 = anyRenkuProjectEntities
        .modify(replaceProjectDateCreated(timestamps(max = migrationDate).generateAs(projects.DateCreated)))
        .map(setSchema(v9))
        .generateOne
      val v10Project = anyRenkuProjectEntities.map(setSchema(v10)).generateOne
      val v9Project2 = anyRenkuProjectEntities
        .modify(
          replaceProjectDateCreated(
            timestampsNotInTheFuture(butYoungerThan = migrationDate).generateAs(projects.DateCreated)
          )
        )
        .map(setSchema(v9))
        .generateOne

      fetchBacklogProjects shouldBe Nil

      upload(to = projectsDataset, v9Project1, v10Project, v9Project2)(implicitly[EntityFunctions[Project]],
                                                                       projectsDSGraphsProducer[Project],
                                                                       ioRuntime
      )

      backlogCreator.createBacklog().unsafeRunSync() shouldBe ()

      fetchBacklogProjects.toSet shouldBe Set(v9Project1.path, v9Project2.path)
    }

    "skip projects without schema but dateCreated before the migration start time" in new TestCase {

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

      fetchBacklogProjects shouldBe Nil

      upload(to = projectsDataset, oldProject1, newProject, oldProject2)(implicitly[EntityFunctions[NonRenkuProject]],
                                                                         projectsDSGraphsProducer[NonRenkuProject],
                                                                         ioRuntime
      )

      backlogCreator.createBacklog().unsafeRunSync() shouldBe ()

      fetchBacklogProjects.toSet shouldBe Set(oldProject1.path, oldProject2.path)
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val migrationDateFinder = mock[MigrationStartTimeFinder[IO]]
    val backlogCreator = new BacklogCreatorImpl[IO](migrationDateFinder,
                                                    RecordsFinder[IO](projectsDSConnectionInfo),
                                                    TSClient[IO](migrationsDSConnectionInfo)
    )

    def givenMigrationDateFinding(returning: IO[Instant]) =
      (() => migrationDateFinder.findMigrationStartDate)
        .expects()
        .returning(returning)
        .atLeastOnce()

    def fetchBacklogProjects =
      runSelect(
        on = migrationsDataset,
        SparqlQuery.ofUnsafe(
          "test V10 backlog",
          Prefixes of renku -> "renku",
          s"""|SELECT ?path
              |WHERE {
              |  ${MigrationToV10.name.asEntityId.asSparql.sparql} renku:toBeMigrated ?path
              |}
              |""".stripMargin
        )
      ).unsafeRunSync().flatMap(_.get("path").map(projects.Path))
  }

  private def setSchema(version: SchemaVersion): Project => Project =
    _.fold(_.copy(version = version), _.copy(version = version), identity, identity)
}
