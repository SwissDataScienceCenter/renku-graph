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
package projectslug

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RecordsFinder

class BacklogCreatorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers
    with TSTooling {

  private val pageSize = 50

  it should "find all projects that have no renku:slug, either DiscoverableProject or Project entities " +
    "and copy their paths into the migrations DS" in allDSConfigs.use {
      case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
        val projects = anyProjectEntities
          .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)

        fetchBacklogProjects.asserting(_ shouldBe Nil) >>
          provision(projects).assertNoException >>
          deleteRenkuSlugProps(projects).assertNoException >>
          backlogCreator.createBacklog().assertNoException >>
          fetchBacklogProjects.asserting(_.toSet shouldBe projects.map(_.slug).toSet)
    }

  it should "skip projects that already have dateModified" in allDSConfigs.use {
    case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
      val projectToSkip    = anyProjectEntities.generateOne
      val projectNotToSkip = anyProjectEntities.generateOne

      provision(projectToSkip).assertNoException >>
        provision(projectNotToSkip).assertNoException >>
        fetchBacklogProjects.asserting(_ shouldBe Nil) >>
        deleteRenkuSlugProp(projectNotToSkip.resourceId).assertNoException >>
        backlogCreator.createBacklog().assertNoException >>
        fetchBacklogProjects.asserting(_.toSet shouldBe Set(projectNotToSkip.slug))
  }

  it should "find project that does not have dateModified only in the Project graph" in allDSConfigs.use {
    case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
      val project = anyProjectEntities.generateOne

      provision(project).assertNoException >>
        fetchBacklogProjects.asserting(_ shouldBe Nil) >>
        deleteProjectRenkuSlug(project.resourceId).assertNoException >>
        backlogCreator.createBacklog().assertNoException >>
        fetchBacklogProjects.asserting(_.toSet shouldBe Set(project.slug))
  }

  it should "find project that does not have dateModified only in the Projects graph" in allDSConfigs.use {
    case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
      val project = anyProjectEntities.generateOne

      provision(project).assertNoException >>
        fetchBacklogProjects.asserting(_ shouldBe Nil) >>
        deleteProjectsRenkuSlug(project.resourceId).assertNoException >>
        backlogCreator.createBacklog().assertNoException >>
        fetchBacklogProjects.asserting(_.toSet shouldBe Set(project.slug))
  }

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()

  private implicit lazy val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private def backlogCreator(implicit pcc: ProjectsConnectionConfig, mcc: MigrationsConnectionConfig) =
    new BacklogCreatorImpl[IO](RecordsFinder[IO](pcc), TSClient[IO](mcc))

  private def fetchBacklogProjects(implicit mcc: MigrationsConnectionConfig): IO[List[projects.Slug]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test Projects renku:slug",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?slug
                 |WHERE {
                 |  ${AddProjectSlug.name.asEntityId} renku:toBeMigrated ?slug
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("slug").map(projects.Slug)))

  private def provision(project: Project)(implicit pcc: ProjectsConnectionConfig): IO[Unit] =
    provision(List(project))

  private def provision(projects: List[Project])(implicit pcc: ProjectsConnectionConfig): IO[Unit] =
    provisionTestProjects(projects: _*)
}
