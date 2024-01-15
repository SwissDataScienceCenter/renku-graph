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

package io.renku.entities.searchgraphs.datasets.commands

import Encoders._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, datasets, entities}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig, SparqlQuery}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class SlugVisibilitiesConcatDeleteQuerySpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers {

  it should "delete the renku:projectsVisibilitiesConcat triple of the given DS" in projectsDSConfig.use {
    implicit pcc =>
      val project = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      val info    = datasetSearchInfoObjects(project).generateOne

      insert(info.asQuads.toList) >>
        findSlugVisibilities(info.topmostSameAs).asserting(_.isDefined shouldBe true) >>
        runUpdate(SlugVisibilitiesConcatDeleteQuery(info.topmostSameAs)).assertNoException >>
        findSlugVisibilities(info.topmostSameAs).asserting(_.isDefined shouldBe false)
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()

  private def findSlugVisibilities(topSameAs: datasets.TopmostSameAs)(implicit pcc: ProjectsConnectionConfig) =
    runSelect(query(topSameAs))
      .map(_.headOption.flatMap(_.get("vis")))

  private def query(topSameAs: datasets.TopmostSameAs) =
    SparqlQuery.ofUnsafe(
      "dataset info projectsVisibilitiesConcat",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?vis
               |WHERE {
               |  GRAPH ${GraphClass.Datasets.id} {
               |    ${topSameAs.asEntityId} renku:projectsVisibilitiesConcat ?vis
               |  }
               |}
               |""".stripMargin
    )
}
