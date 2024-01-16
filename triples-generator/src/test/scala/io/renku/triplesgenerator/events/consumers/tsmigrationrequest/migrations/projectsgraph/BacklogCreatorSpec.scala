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
package projectsgraph

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
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger
import tooling._

class BacklogCreatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers {

  private val pageSize = 50

  "createBacklog" should {

    "find all projects that have relevant Project graphs but no DiscoverableProject entity in the Projects graph " +
      "and copy their slugs into the migrations DS" in allDSConfigs.use {
        case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
          val projects = anyProjectEntities
            .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)

          for {
            _ <- fetchBacklogProjects.asserting(_ shouldBe Nil)

            _ <- uploadToProjects(projects: _*)

            _ <- backlogCreator.createBacklog().assertNoException

            _ <- fetchBacklogProjects.asserting(_.toSet shouldBe projects.map(_.slug).toSet)
          } yield Succeeded
      }

    "skip projects that are already in the Projects graph" in allDSConfigs.use {
      case (implicit0(f: ProjectsConnectionConfig), implicit0(d: MigrationsConnectionConfig)) =>
        val projectToSkip = anyProjectEntities.generateOne
        for {
          _ <- provisionTestProject(projectToSkip)

          projectNotToSkip = anyProjectEntities.generateOne
          _ <- uploadToProjects(projectNotToSkip)

          _ <- fetchBacklogProjects.asserting(_ shouldBe Nil)

          _ <- backlogCreator.createBacklog().assertNoException

          _ <- fetchBacklogProjects.asserting(_.toSet shouldBe Set(projectNotToSkip.slug))
        } yield Succeeded
    }
  }

  implicit override val ioLogger: Logger[IO] = TestLogger[IO]()

  private def backlogCreator(implicit pcc: ProjectsConnectionConfig, mcc: MigrationsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    new BacklogCreatorImpl[IO](RecordsFinder[IO](pcc), TSClient[IO](mcc))
  }

  def fetchBacklogProjects(implicit mcc: MigrationsConnectionConfig) =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test Projects Graph provisioning backlog",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?slug
                 |WHERE {
                 |  ${ProvisionProjectsGraph.name.asEntityId} renku:toBeMigrated ?slug
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("slug").map(projects.Slug)))
}
