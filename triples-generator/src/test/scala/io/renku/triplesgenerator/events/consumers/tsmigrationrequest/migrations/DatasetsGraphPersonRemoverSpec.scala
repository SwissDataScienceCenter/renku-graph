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

import cats.data.NonEmptyList
import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.TestMetricsRegistry
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import tooling.RegisteredUpdateQueryMigration

class DatasetsGraphPersonRemoverSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with AsyncMockFactory {

  it should "be a RegisteredUpdateQueryMigration" in {
    implicit val metricsRegistry: TestMetricsRegistry[IO]     = TestMetricsRegistry[IO]
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

    DatasetsGraphPersonRemover[IO].asserting(
      _.getClass shouldBe classOf[RegisteredUpdateQueryMigration[IO]]
    )
  }

  it should "remove Person entities from the Datasets graph" in {

    val ds1 -> project1 = anyRenkuProjectEntities.addDataset(datasetEntities(provenanceInternal)).generateOne
    val ds2 -> project2 = anyRenkuProjectEntities.addDataset(datasetEntities(provenanceInternal)).generateOne

    val allNames = (ds1.provenance.creators.toList ::: ds2.provenance.creators.toList).map(_.name).toSet

    provisionTestProjects(project1, project2).assertNoException >>
      insertIO(projectsDataset, toQuads(ds1.provenance.creators)).assertNoException >>
      insertIO(projectsDataset, toQuads(ds2.provenance.creators)).assertNoException >>
      fetchCreatorsNames.asserting(_.toSet shouldBe allNames) >>
      runUpdate(projectsDataset, DatasetsGraphPersonRemover.query).assertNoException >>
      fetchCreatorsNames.asserting(_ shouldBe List.empty)
  }

  private def toQuads(persons: NonEmptyList[Person]): List[Quad] =
    persons.toList.flatMap(toSinglePersonQuads)

  private def toSinglePersonQuads(person: Person): List[Quad] = List(
    Quad(GraphClass.Datasets.id, person.resourceId.asEntityId, rdf / "type", entities.Person.Ontology.typeClass.id),
    Quad(GraphClass.Datasets.id,
         person.resourceId.asEntityId,
         entities.Person.Ontology.nameProperty.id,
         person.name.asObject
    )
  )

  private def fetchCreatorsNames: IO[List[persons.Name]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test creator name",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?name
                 |WHERE {
                 |   GRAPH ${GraphClass.Datasets.id} {
                 |     ?id a schema:Person;
                 |         schema:name ?name
                 |   }
                 |}
                 |""".stripMargin
      )
    ).map(_.flatMap(_.get("name").map(persons.Name)))

  implicit override lazy val ioLogger: Logger[IO] = TestLogger[IO]()
}
