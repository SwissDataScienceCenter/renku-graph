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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package datasets

import cats.syntax.all._
import ch.datascience.graph.Schemas._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import monocle.function.Plated

private class TriplesUpdater {

  def mergeTopmostDataIntoTriples[Interpretation[_]](
      curatedTriples: CuratedTriples[Interpretation],
      topmostData:    TopmostData
  ): CuratedTriples[Interpretation] = curatedTriples.copy(
    triples = JsonLDTriples(Plated.transform(updateDataset(topmostData))(curatedTriples.triples.value))
  )

  private def updateDataset(topmostData: TopmostData): Json => Json = { json =>
    root.`@type`.each.string.getAll(json) match {
      case types if types contains (schema / "Dataset").show =>
        json.get[EntityId]("@id") match {
          case Some(topmostData.datasetId) =>
            val noSameAsJson  = json.remove(renku / "topmostSameAs")
            val noTopmostJson = noSameAsJson.remove(renku / "topmostDerivedFrom")
            noTopmostJson deepMerge Json.obj(
              (renku / "topmostSameAs").toString      -> topmostData.topmostSameAs.asJsonLD.toJson,
              (renku / "topmostDerivedFrom").toString -> topmostData.topmostDerivedFrom.asJsonLD.toJson
            )
          case _ => json
        }
      case _ => json
    }
  }
}
