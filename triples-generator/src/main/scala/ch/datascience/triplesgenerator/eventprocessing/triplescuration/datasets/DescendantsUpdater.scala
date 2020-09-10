/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.MonadError
import ch.datascience.graph.model.datasets.{DerivedFrom, TopmostSameAs}
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId

import scala.language.higherKinds

private class DescendantsUpdater {

  def prepareUpdates[Interpretation[_]](
      curatedTriples: CuratedTriples[Interpretation],
      topmostData:    TopmostData
  )(implicit ME:      MonadError[Interpretation, Throwable]): CuratedTriples[Interpretation] = curatedTriples.copy(
    updates =
      curatedTriples.updates ++: List(
        prepareSameAsUpdate(topmostData.datasetId, topmostData.sameAs),
        prepareDerivedFromUpdate(topmostData.datasetId, topmostData.derivedFrom)
      )
  )

  private def prepareSameAsUpdate[Interpretation[_]](
      entityId:      EntityId,
      topmostSameAs: TopmostSameAs
  )(implicit ME:     MonadError[Interpretation, Throwable]) =
    UpdateFunction[Interpretation](
      s"Updating Dataset $entityId topmostSameAs",
      SparqlQuery(
        "upload - topmostSameAs update",
        Set(
          "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
          "PREFIX schema: <http://schema.org/>"
        ),
        s"""|DELETE { ?dsId renku:topmostSameAs <$entityId> }
            |INSERT { ?dsId renku:topmostSameAs <$topmostSameAs> }
            |WHERE {
            |  ?dsId rdf:type schema:Dataset;
            |        renku:topmostSameAs <$entityId>.
            |}
            |""".stripMargin
      )
    )

  private def prepareDerivedFromUpdate[Interpretation[_]](
      entityId:           EntityId,
      topmostDerivedFrom: DerivedFrom
  )(implicit ME:          MonadError[Interpretation, Throwable]) = UpdateFunction[Interpretation](
    s"Updating Dataset $entityId topmostDerivedFrom",
    SparqlQuery(
      "upload - topmostDerivedFrom update",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
        "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
        "PREFIX schema: <http://schema.org/>"
      ),
      s"""|DELETE { ?dsId renku:topmostDerivedFrom <$entityId> }
          |INSERT { ?dsId renku:topmostDerivedFrom <$topmostDerivedFrom> }
          |WHERE {
          |  ?dsId rdf:type schema:Dataset;
          |        renku:topmostDerivedFrom <$entityId>
          |}
          |""".stripMargin
    )
  )
}
