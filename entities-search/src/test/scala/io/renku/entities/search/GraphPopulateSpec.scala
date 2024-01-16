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

package io.renku.entities.search

import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GraphClass, Schemas}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{GraphJenaSpec, SparqlQuery}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class GraphPopulateSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with FinderSpec
    with TestSearchInfoDatasets
    with EntitiesGenerators
    with should.Matchers {

  it should "populate the schema:Datasets graph" in projectsDSConfig.use { implicit pcc =>
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    val query =
      SparqlQuery.of(
        "test query",
        Prefixes.of(Schemas.renku -> "renku", Schemas.schema -> "schema"),
        sparql"""|select (count(?id) as ?count)
                 |where {
                 |  graph ${GraphClass.Datasets.id} {
                 |    ?id ?p ?o
                 |  }
                 |}
                 |""".stripMargin
      )

    provisionTestProject(project) >>
      runSelect(query).asserting(_.head("count").toLong should be > 1L)
  }
}
