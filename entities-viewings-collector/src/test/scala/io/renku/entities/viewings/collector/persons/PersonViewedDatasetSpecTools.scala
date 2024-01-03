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

package io.renku.entities.viewings.collector.persons

import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.{GraphClass, datasets, entities, persons}
import io.renku.jsonld.syntax._
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.graph.model.views.TinyTypeToObject._

import java.time.Instant

trait PersonViewedDatasetSpecTools {
  self: InMemoryJenaForSpec with ProjectsDataset with IOSpec =>

  protected def findAllViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find user dataset viewings",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?id ?datasetId ?date
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  ?id renku:viewedDataset ?viewingId.
                 |  ?viewingId renku:dataset ?datasetId;
                 |             renku:dateViewed ?date.
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row =>
        ViewingRecord(persons.ResourceId(row("id")),
                      datasets.ResourceId(row("datasetId")),
                      datasets.DateViewed(Instant.parse(row("date")))
        )
      )
      .toSet

  protected case class ViewingRecord(userId:    persons.ResourceId,
                                     datasetId: datasets.ResourceId,
                                     date:      datasets.DateViewed
  )

  protected def toCollectorDataset(ds: entities.Dataset[entities.Dataset.Provenance]) =
    collector.persons.Dataset(ds.resourceId, ds.identification.identifier)

  protected def insertOtherDate(datasetId: datasets.ResourceId, dateViewed: datasets.DateViewed) =
    runUpdate(
      on = projectsDataset,
      SparqlQuery.of(
        "test add another user dataset dateViewed",
        Prefixes of renku -> "renku",
        sparql"""|INSERT {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ?viewingId renku:dateViewed ${dateViewed.asObject}
                 |  }
                 |}
                 |WHERE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ?viewingId renku:dataset ${datasetId.asEntityId}
                 |  }
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
}
