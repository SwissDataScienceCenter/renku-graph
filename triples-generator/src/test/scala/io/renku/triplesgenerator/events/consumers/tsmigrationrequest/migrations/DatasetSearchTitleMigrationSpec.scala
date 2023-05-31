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

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

class DatasetSearchTitleMigrationSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with SearchInfoDatasets
    with ProjectsDataset {

  private val prefixes = Prefixes.of(schema -> "schema", renku -> "renku")
  implicit val ioLogger: Logger[IO] = TestLogger()

  "run" should {
    "Add datasets title into the search graph" in {
      val data = anyRenkuProjectEntities
        .modify(replaceProjectCreator(personEntities.generateSome))
        .addDataset(datasetEntities(provenanceInternal))
        .generateList(min = 2, max = 2)

      provisionTestProjects(data.map(_._2): _*).unsafeRunSync()

      // initially, names are provisioned
      val titles = findDatasetNames.unsafeRunSync()
      titles should not be empty

      // remove names
      removeDatasetNames.unsafeRunSync()
      findDatasetNames.unsafeRunSync() should be(empty)

      // run the migration
      runUpdate(projectsDataset, DatasetSearchTitleMigration.query).unsafeRunSync()

      // names should be back
      val nextTitles = findDatasetNames.unsafeRunSync()
      nextTitles shouldBe titles
    }
  }

  def findDatasetNames: IO[Map[datasets.TopmostSameAs, List[datasets.Name]]] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "find dates",
        prefixes,
        """
          |SELECT ?sameAs ?name
          |  WHERE {
          |    Graph schema:Dataset {
          |      ?sameAs a renku:DiscoverableDataset;
          |              schema:name ?name
          |    }
          |  }
          |""".stripMargin
      )
    ).map(
      _.map(row => datasets.TopmostSameAs(row("sameAs")) -> datasets.Name(row("name")))
        .groupMap(_._1)(_._2)
    )

  def removeDatasetNames =
    runUpdate(
      projectsDataset,
      SparqlQuery.of(
        "delete names",
        prefixes,
        """|DELETE {
           |  Graph schema:Dataset {
           |    ?sameAs schema:name ?name
           |  }
           |}
           |WHERE {
           |  Graph schema:Dataset {
           |    ?sameAs a renku:DiscoverableDataset;
           |            schema:name ?name.
           |  }
           |}
           |""".stripMargin
      )
    )
}
