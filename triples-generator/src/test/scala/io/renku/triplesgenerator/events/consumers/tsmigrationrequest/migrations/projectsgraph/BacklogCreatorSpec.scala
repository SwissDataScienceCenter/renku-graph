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
package projectsgraph

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger
import tooling._

class BacklogCreatorSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MigrationsDataset
    with SearchInfoDatasets
    with MockFactory {

  private val pageSize = 50

  "createBacklog" should {

    "find all projects that have relevant Project graphs but no DiscoverableProject entity in the Projects graph " +
      "and copy their slugs into the migrations DS" in new TestCase {

        val projects = anyProjectEntities
          .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)

        fetchBacklogProjects shouldBe Nil

        upload(to = projectsDataset, projects: _*)(implicitly[EntityFunctions[Project]],
                                                   projectsDSGraphsProducer[Project],
                                                   ioRuntime
        )

        backlogCreator.createBacklog().unsafeRunSync() shouldBe ()

        fetchBacklogProjects.toSet shouldBe projects.map(_.slug).toSet
      }

    "skip projects that are already in the Projects graph" in new TestCase {

      val projectToSkip = anyProjectEntities.generateOne
      provisionTestProject(projectToSkip)(implicitly[RenkuUrl],
                                          implicitly[EntityFunctions[entities.Project]],
                                          projectsDSGraphsProducer[entities.Project]
      ).unsafeRunSync()

      val projectNotToSkip = anyProjectEntities.generateOne
      upload(to = projectsDataset, projectNotToSkip)(implicitly[EntityFunctions[Project]],
                                                     projectsDSGraphsProducer[Project],
                                                     ioRuntime
      )

      fetchBacklogProjects shouldBe Nil

      backlogCreator.createBacklog().unsafeRunSync() shouldBe ()

      fetchBacklogProjects.toSet shouldBe Set(projectNotToSkip.slug)
    }
  }

  private implicit val logger:    TestLogger[IO] = TestLogger[IO]()
  implicit override val ioLogger: Logger[IO]     = logger

  private trait TestCase {

    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val backlogCreator =
      new BacklogCreatorImpl[IO](RecordsFinder[IO](projectsDSConnectionInfo), TSClient[IO](migrationsDSConnectionInfo))

    def fetchBacklogProjects =
      runSelect(
        on = migrationsDataset,
        SparqlQuery.ofUnsafe(
          "test Projects Graph provisioning backlog",
          Prefixes of renku -> "renku",
          sparql"""|SELECT ?slug
                   |WHERE {
                   |  ${ProvisionProjectsGraph.name.asEntityId} renku:toBeMigrated ?slug
                   |}
                   |""".stripMargin
        )
      ).unsafeRunSync().flatMap(_.get("slug").map(projects.Slug))
  }
}
