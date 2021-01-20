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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.datasets

import ch.datascience.generators.CommonGraphGenerators.fusekiBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.rdfstore.entities.DataSet
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{FusekiBaseUrl, JsonLDTriples}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.CurationGenerators.curatedTriplesObjects
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.optics.{JsonPath, JsonTraversalPath}
import io.renku.jsonld.{EntityId, Property}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TriplesUpdaterSpec extends AnyWordSpec with should.Matchers {

  private implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

  "mergeTopmostDataIntoTriples" should {

    "add given topmost SameAs and DerivedFrom to Dataset entity in the given triples" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      val datasetId  = DataSet.entityId(identifier)(renkuBaseUrl)
      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = None).toJson
      }

      val topmostData = topmostDatas(datasetId).generateOne

      val updatedTriples = updater.mergeTopmostDataIntoTriples[Try](curatedTriples.copy(triples = triples), topmostData)

      val Some(updatedDataset) = updatedTriples.triples.findDataset(datasetId)

      findIdentifier(updatedDataset)         shouldBe Some(identifier.toString)
      findTopmostSameAs(updatedDataset)      shouldBe Some(topmostData.topmostSameAs.toString)
      findTopmostDerivedFrom(updatedDataset) shouldBe Some(topmostData.topmostDerivedFrom.toString)

      updatedTriples.updatesGroups shouldBe curatedTriples.updatesGroups
    }
  }

  private trait TestCase {
    val curatedTriples = curatedTriplesObjects[Try].generateOne

    val updater = new TriplesUpdater()
  }

  private def findIdentifier(json: Json) = (root / (schema / "identifier")).`@value`.string.getOption(json)

  private def findTopmostSameAs(json: Json) =
    (root / (renku / "topmostSameAs")).`@id`.string.getOption(json)

  private def findTopmostDerivedFrom(json: Json) =
    (root / (renku / "topmostDerivedFrom")).`@id`.string.getOption(json)

  private def topmostDatas(datasetId: EntityId) =
    for {
      topmostSameAs      <- datasetTopmostSameAs
      topmostDerivedFrom <- datasetTopmostDerivedFroms
    } yield TopmostData(datasetId, topmostSameAs, topmostDerivedFrom)

  private implicit class JsonLdOps(jsonLd: JsonLDTriples) {
    private val json = jsonLd.value

    private val id = root.`@id`.string
    private val datasetObject =
      ((root.`@reverse` / (prov / "activity")).each.`@reverse` / (prov / "qualifiedGeneration")).json

    def findDataset(datasetId: EntityId): Option[Json] =
      datasetObject
        .getAll(json)
        .find(datasetObj => id.getOption(datasetObj).contains(datasetId.toString))
  }

  private implicit class JsonPathOps(jsonPath: JsonPath) {
    def /(property: Property): JsonPath = jsonPath.selectDynamic(property.toString)
  }

  private implicit class JsonTraversalPathOps(jsonPath: JsonTraversalPath) {
    def /(property: Property): JsonTraversalPath = jsonPath.selectDynamic(property.toString)
  }
}
