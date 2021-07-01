/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.MonadThrow
import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.datasets.{TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId

private class DescendantsUpdater {

  def prepareUpdates[Interpretation[_]: MonadThrow](
      curatedTriples: CuratedTriples[Interpretation],
      topmostData:    TopmostData
  ): CuratedTriples[Interpretation] = curatedTriples.copy(
    updatesGroups = curatedTriples.updatesGroups ::: CurationUpdatesGroup(
      name = "Dataset descendants updates",
      prepareSameAsUpdate(topmostData.datasetId, topmostData.topmostSameAs),
      prepareDerivedFromUpdate(topmostData.datasetId, topmostData.topmostDerivedFrom)
    ) :: Nil
  )

  private def prepareSameAsUpdate(entityId: EntityId, topmostSameAs: TopmostSameAs) = SparqlQuery.of(
    name = "upload - topmostSameAs update",
    Prefixes.of(renku -> "renku", schema -> "schema"),
    s"""|DELETE { ?dsId renku:topmostSameAs <$entityId> }
        |INSERT { ?dsId renku:topmostSameAs <$topmostSameAs> }
        |WHERE {
        |  ?dsId a schema:Dataset;
        |        renku:topmostSameAs <$entityId>.
        |}
        |""".stripMargin
  )

  private def prepareDerivedFromUpdate(entityId: EntityId, topmostDerivedFrom: TopmostDerivedFrom) = SparqlQuery.of(
    name = "upload - topmostDerivedFrom update",
    Prefixes.of(renku -> "renku", schema -> "schema"),
    s"""|DELETE { ?dsId renku:topmostDerivedFrom <$entityId> }
        |INSERT { ?dsId renku:topmostDerivedFrom <$topmostDerivedFrom> }
        |WHERE {
        |  ?dsId a schema:Dataset;
        |        renku:topmostDerivedFrom <$entityId>
        |}
        |""".stripMargin
  )
}
