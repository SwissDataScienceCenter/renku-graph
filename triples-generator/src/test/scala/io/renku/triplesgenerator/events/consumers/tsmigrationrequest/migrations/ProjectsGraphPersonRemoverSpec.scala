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
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RegisteredUpdateQueryMigration

class ProjectsGraphPersonRemoverSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers
    with AsyncMockFactory {

  it should "be a RegisteredUpdateQueryMigration" in {
    implicit val metricsRegistry: TestMetricsRegistry[IO]     = TestMetricsRegistry[IO]
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

    ProjectsGraphPersonRemover[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
  }

  it should "remove Person entities from the Projects graph" in projectsDSConfig.use { implicit pcc =>
    val project1Creator = personEntities(withGitLabId).generateOne
    val project1 = anyProjectEntities.map(replaceProjectCreator(project1Creator.some)).generateOne.to[entities.Project]
    val project2Creator = personEntities(withGitLabId).generateOne
    val project2 = anyProjectEntities.map(replaceProjectCreator(project2Creator.some)).generateOne.to[entities.Project]

    provisionProjects(project1, project2).assertNoException >>
      insert(toQuads(project1Creator)).assertNoException >>
      insert(toQuads(project2Creator)).assertNoException >>
      fetchCreatorName(project1Creator.resourceId).asserting(_ shouldBe List(project1Creator.name)) >>
      fetchCreatorName(project2Creator.resourceId).asserting(_ shouldBe List(project2Creator.name)) >>
      runUpdate(ProjectsGraphPersonRemover.query).assertNoException >>
      fetchCreatorName(project1Creator.resourceId).asserting(_ shouldBe List.empty) >>
      fetchCreatorName(project2Creator.resourceId).asserting(_ shouldBe List.empty)
  }

  private def toQuads(person: Person): List[Quad] = List(
    Quad(GraphClass.Projects.id, person.resourceId.asEntityId, rdf / "type", entities.Person.Ontology.typeClass.id),
    Quad(GraphClass.Projects.id,
         person.resourceId.asEntityId,
         entities.Person.Ontology.nameProperty.id,
         person.name.asObject
    )
  )

  private def fetchCreatorName(personId: persons.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[List[persons.Name]] =
    runSelect(
      SparqlQuery.ofUnsafe(
        "test creator name",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?name
                 |WHERE {
                 |   GRAPH ${GraphClass.Projects.id} {
                 |     ${personId.asEntityId} schema:name ?name
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("name").map(persons.Name)))

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()
}
