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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.TestMetricsRegistry
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RegisteredUpdateQueryMigration

class ProjectMembersRemoverSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers
    with AsyncMockFactory {

  it should "be a RegisteredUpdateQueryMigration" in {
    implicit val metricsRegistry: TestMetricsRegistry[IO]     = TestMetricsRegistry[IO]
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

    ProjectMembersRemover[IO].asserting(_.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]])
  }

  it should "remove schema:member properties from all Project graphs" in projectsDSConfig.use { implicit pcc =>
    val project1 = anyRenkuProjectEntities
      .modify(replaceMembers(projectMemberEntities(withGitLabId).generateSet(min = 1)))
      .generateOne
      .to[entities.Project]
    val project2 = anyRenkuProjectEntities
      .modify(replaceMembers(projectMemberEntities(withGitLabId).generateSet(min = 1)))
      .generateOne
      .to[entities.Project]

    for {
      _ <- provisionProjects(project1, project2).assertNoException

      _ <- membersIdsInserts(project1).toList.map(runUpdate).sequence.assertNoException
      _ <- membersIdsInserts(project2).toList.map(runUpdate).sequence.assertNoException

      _ <- fetchMembersIds(project1.resourceId).asserting(_ shouldBe project1.members.map(_.person.resourceId))
      _ <- fetchMembersIds(project2.resourceId).asserting(_ shouldBe project2.members.map(_.person.resourceId))

      _ <- runUpdate(ProjectMembersRemover.query).assertNoException

      _ <- fetchMembersIds(project1.resourceId).asserting(_ shouldBe Set.empty)
      _ <- fetchMembersIds(project2.resourceId).asserting(_ shouldBe Set.empty)
    } yield Succeeded
  }

  private def fetchMembersIds(id: projects.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Set[persons.ResourceId]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test ds members",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?memberId
                 |WHERE {
                 |   BIND (${id.asEntityId} AS ?id)
                 |   GRAPH ?id {
                 |     ?id a schema:Project;
                 |         schema:member ?memberId.
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(_.map(row => persons.ResourceId(row("memberId"))).toSet)

  private def membersIdsInserts(project: entities.Project) =
    project.members.map { member =>
      SparqlQuery.ofUnsafe(
        "test insert member id",
        Prefixes of schema -> "schema",
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Project.id(project.resourceId)} {
                 |    ${project.resourceId.asEntityId} schema:member ${member.person.resourceId.asEntityId}.
                 |  }
                 |}
                 |""".stripMargin
      )
    }

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()
}
