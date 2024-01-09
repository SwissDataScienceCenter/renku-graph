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
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.typelevel.log4cats.Logger

class DatasetSearchTitleMigrationSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers {

  private val prefixes = Prefixes of (schema -> "schema", renku -> "renku")
  implicit val ioLogger: Logger[IO] = TestLogger()

  "run" should {

    "add datasets name into the search graph" in projectsDSConfig.use { implicit pcc =>
      val data = anyRenkuProjectEntities
        .modify(replaceProjectCreator(personEntities.generateSome))
        .addDataset(datasetEntities(provenanceInternal))
        .generateList(min = 2, max = 2)

      for {
        _ <- provisionTestProjects(data.map(_._2): _*)

        // initially, names are provisioned
        names <- findDatasetNames
        _ = names should not be empty

        // remove names
        _ <- removeDatasetNames
        _ <- findDatasetNames.asserting(_ should be(empty))

        // run the migration
        _ <- runUpdate(DatasetSearchTitleMigration.query)

        // names should be back
        _ <- findDatasetNames.asserting(_ shouldBe names)
      } yield Succeeded
    }
  }

  private def findDatasetNames(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Map[datasets.TopmostSameAs, List[datasets.Name]]] =
    runSelect(
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

  private def removeDatasetNames(implicit pcc: ProjectsConnectionConfig) =
    runUpdate(
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
