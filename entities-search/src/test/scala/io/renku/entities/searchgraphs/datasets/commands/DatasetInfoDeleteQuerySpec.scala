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

package io.renku.entities.searchgraphs.datasets.commands

import Encoders._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.entities.searchgraphs.datasets.SearchInfoLens._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, datasets, entities}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQuery}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class DatasetInfoDeleteQuerySpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets {

  it should "delete all the triples of a DiscoverableDataset together with all the dependant entities" in {

    val project      = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
    val otherProject = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
    val info = datasetSearchInfoObjects(project).map { si =>
      val linkToOtherProject =
        updateLinkProject(otherProject)(linkObjectsGen(si.topmostSameAs).generateOne)
      searchInfoLinks.modify(_ append linkToOtherProject)(si)
    }.generateOne

    List(project, otherProject).traverse_(insertProjectAuth) >>
      insertIO(projectsDataset, info.asQuads.toList) >>
      triplesCount(projectsDataset, GraphClass.Datasets.id).asserting(_ should be > 0L) >>
      runUpdate(projectsDataset, DatasetInfoDeleteQuery(info.topmostSameAs)).assertNoException >>
      triplesCount(projectsDataset, GraphClass.Datasets.id).asserting(_ shouldBe 0L)
  }

  it should "delete only triples of a DiscoverableDataset with the given topmostSameAs" in {

    val project = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
    val info1   = datasetSearchInfoObjects(project).generateOne
    val info2   = datasetSearchInfoObjects(project).generateOne

    insertIO(projectsDataset, info1.asQuads.toList) >>
      insertIO(projectsDataset, info2.asQuads.toList) >>
      findCount(info1.topmostSameAs).asserting(_ should be > 0) >>
      runUpdate(projectsDataset, DatasetInfoDeleteQuery(info1.topmostSameAs)).assertNoException >>
      findCount(info1.topmostSameAs).asserting(_ shouldBe 0) >>
      findCount(info2.topmostSameAs).asserting(_ should be > 0)
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()

  private def findCount(topSameAs: datasets.TopmostSameAs) =
    runSelect(projectsDataset, countQuery(topSameAs))
      .map(_.headOption.flatMap(_.get("cnt").flatMap(_.toIntOption)).getOrElse(0))

  private def countQuery(topSameAs: datasets.TopmostSameAs) =
    SparqlQuery.ofUnsafe(
      "dataset info triples count",
      Prefixes of schema -> "schema",
      sparql"""|SELECT (COUNT(?topSameAs) AS ?cnt)
               |WHERE {
               |  GRAPH ${GraphClass.Datasets.id} {
               |    BIND (${topSameAs.asEntityId} AS ?topSameAs)
               |    ?topSameAs ?p ?o.
               |  }
               |}
               |""".stripMargin
    )
}
