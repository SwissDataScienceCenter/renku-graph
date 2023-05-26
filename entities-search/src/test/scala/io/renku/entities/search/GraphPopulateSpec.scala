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

package io.renku.entities.search

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GraphClass, Schemas}
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQuery}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class GraphPopulateSpec
    extends AnyFlatSpec
    with should.Matchers
    with EntitiesGenerators
    with FinderSpecOps
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with IOSpec {

  it should "populate the schema:Datasets graph" in {
    val project = renkuProjectEntities(visibilityPublic)
      .withActivities(activityEntities(stepPlanEntities()))
      .withDatasets(datasetEntities(provenanceNonModified))
      .generateOne

    provisionTestProject(project).unsafeRunSync()

    val query =
      SparqlQuery.of(
        "test query",
        Prefixes.of(Schemas.renku -> "renku", Schemas.schema -> "schema"),
        s"""
           |select (count(?id) as ?count)
           |where {
           |  graph <${GraphClass.Datasets.id}> {
           |    ?id ?p ?o
           |  }
           |}
           |""".stripMargin
      )

    val test =
      for {
        qr  <- queryRunnerFor(projectsDataset)
        res <- qr.queryExpecting[List[Map[String, String]]](query)
        _ = res.head("count").toLong should be > 1L
      } yield ()

    test.unsafeRunSync()
  }

  implicit def responseDecoder: Decoder[List[Map[String, String]]] = {
    def decodeRow(vars: List[String]): Decoder[Map[String, String]] =
      Decoder.instance { c =>
        vars
          .traverse(fieldName => c.downField(fieldName).downField("value").as[String].map(v => fieldName -> v))
          .map(_.toMap)
      }

    Decoder.instance { cursor =>
      for {
        vars <- cursor.downField("head").downField("vars").as[List[String]]
        bindings <- cursor
                      .downField("results")
                      .downField("bindings")
                      .as[List[Map[String, String]]](Decoder.decodeList(decodeRow(vars)))
      } yield bindings
    }
  }
}
