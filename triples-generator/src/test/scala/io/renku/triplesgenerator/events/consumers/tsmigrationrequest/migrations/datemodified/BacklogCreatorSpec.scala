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
package datemodified

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RecordsFinder

class BacklogCreatorSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MigrationsDataset
    with SearchInfoDatasets
    with AsyncMockFactory {

  private val pageSize = 50

  it should "find all projects that have no dateModified, either DiscoverableProject or Project entities " +
    "and copy their paths into the migrations DS" in {

      val projects = anyProjectEntities
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)

      fetchBacklogProjects.asserting(_ shouldBe Nil) >>
        provision(projects).assertNoException >>
        deleteModifiedDates(projects).assertNoException >>
        backlogCreator.createBacklog().assertNoException >>
        fetchBacklogProjects.asserting(_.toSet shouldBe projects.map(_.path).toSet)
    }

  it should "skip projects that already have dateModified" in {

    val projectToSkip    = anyProjectEntities.generateOne
    val projectNotToSkip = anyProjectEntities.generateOne

    provision(projectToSkip).assertNoException >>
      provision(projectNotToSkip).assertNoException >>
      fetchBacklogProjects.asserting(_ shouldBe Nil) >>
      deleteModifiedDates(projectNotToSkip.resourceId).assertNoException >>
      backlogCreator.createBacklog().assertNoException >>
      fetchBacklogProjects.asserting(_.toSet shouldBe Set(projectNotToSkip.path))
  }

  it should "find project that does not have dateModified only in the Project graph" in {

    val project = anyProjectEntities.generateOne

    provision(project).assertNoException >>
      fetchBacklogProjects.asserting(_ shouldBe Nil) >>
      deleteProjectDateModified(project.resourceId).assertNoException >>
      backlogCreator.createBacklog().assertNoException >>
      fetchBacklogProjects.asserting(_.toSet shouldBe Set(project.path))
  }

  it should "find project that does not have dateModified only in the Projects graph" in {

    val project = anyProjectEntities.generateOne

    provision(project).assertNoException >>
      fetchBacklogProjects.asserting(_ shouldBe Nil) >>
      deleteProjectsDateModified(project.resourceId).assertNoException >>
      backlogCreator.createBacklog().assertNoException >>
      fetchBacklogProjects.asserting(_.toSet shouldBe Set(project.path))
  }

  private implicit lazy val logger:    TestLogger[IO] = TestLogger[IO]()
  implicit override lazy val ioLogger: Logger[IO]     = logger

  private implicit lazy val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val backlogCreator =
    new BacklogCreatorImpl[IO](RecordsFinder[IO](projectsDSConnectionInfo), TSClient[IO](migrationsDSConnectionInfo))

  private def fetchBacklogProjects: IO[List[projects.Path]] =
    runSelect(
      on = migrationsDataset,
      SparqlQuery.ofUnsafe(
        "test Projects dateModified",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?path
                 |WHERE {
                 |  ${AddProjectDateModified.name.asEntityId} renku:toBeMigrated ?path
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("path").map(projects.Path)))

  private def deleteModifiedDates(projects: List[Project]): IO[Unit] =
    projects.traverse_(p => deleteModifiedDates(p.resourceId))

  private def deleteModifiedDates(id: projects.ResourceId): IO[Unit] =
    deleteProjectDateModified(id) >> deleteProjectsDateModified(id)

  private def deleteProjectDateModified(id: projects.ResourceId): IO[Unit] =
    runUpdate(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test dateModified delete from Project",
        Prefixes of schema -> "schema",
        sparql"""|DELETE {
                 |  GRAPH ?id { ?id schema:dateModified ?dm }
                 |}
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  GRAPH ?id { ?id schema:dateModified ?dm }
                 |}
                 |""".stripMargin
      )
    )

  private def deleteProjectsDateModified(id: projects.ResourceId): IO[Unit] =
    runUpdate(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test dateModified delete from Project",
        Prefixes of schema -> "schema",
        sparql"""|DELETE {
                 |  GRAPH ?gid { ?id schema:dateModified ?dm }
                 |}
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  BIND (${GraphClass.Projects.id} AS ?gid)
                 |  GRAPH ?gid { ?id schema:dateModified ?dm }
                 |}
                 |""".stripMargin
      )
    )

  private def provision(project: Project): IO[Unit] =
    provision(List(project))

  private def provision(projects: List[Project]): IO[Unit] =
    provisionTestProjects(projects: _*)(implicitly[RenkuUrl],
                                        implicitly[EntityFunctions[entities.Project]],
                                        projectsDSGraphsProducer[entities.Project]
    )
}
