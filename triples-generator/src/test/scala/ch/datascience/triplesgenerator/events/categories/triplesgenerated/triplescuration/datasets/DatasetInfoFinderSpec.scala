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

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.jsonLDTriples
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.datasets.{ExternalSameAs, InternalSameAs}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.rdfstore.entities._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class DatasetInfoFinderSpec extends AnyWordSpec with should.Matchers {

  "findDatasetsInfo" should {

    "return empty set if there's no Dataset entity in the Json" in {
      infoFinder.findDatasetsInfo(jsonLDTriples.generateOne) shouldBe Set.empty.pure[Try]
    }

    "return the DatasetInfo without sameAs and derivedFrom when they are not present in the json" in {
      val dataset = datasetEntities(datasetProvenanceInternal).generateOne

      infoFinder.findDatasetsInfo(JsonLDTriples(JsonLD.arr(dataset.asJsonLD).toJson)) shouldBe Set(
        (dataset.asEntityId, None, None)
      ).pure[Try]
    }

    "return the DatasetInfo with UrlSameAs and no derivedFrom as reflected in json" in {
      val dataset = datasetEntities(datasetProvenanceImportedExternal).generateOne

      val Success(datasetInfos) = infoFinder.findDatasetsInfo(JsonLDTriples(JsonLD.arr(dataset.asJsonLD).toJson))

      datasetInfos             shouldBe Set((dataset.asEntityId, dataset.provenance.sameAs.some, None))
      datasetInfos.head._2.get shouldBe an[ExternalSameAs]
    }

    "return the DatasetInfo with IdSameAs and no derivedFrom as reflected in json" in {
      val dataset = datasetEntities(datasetProvenanceImportedInternalAncestorInternal).generateOne

      val Success(datasetInfos) = infoFinder.findDatasetsInfo(JsonLDTriples(JsonLD.arr(dataset.asJsonLD).toJson))

      datasetInfos             shouldBe Set((dataset.asEntityId, dataset.provenance.sameAs.some, None))
      datasetInfos.head._2.get shouldBe an[InternalSameAs]
    }

    "return the DatasetInfo with derivedFrom and no sameAs as reflected in json" in {
      val dataset = datasetEntities(datasetProvenanceModified).generateOne

      infoFinder.findDatasetsInfo(JsonLDTriples(JsonLD.arr(dataset.asJsonLD).toJson)) shouldBe Set(
        (dataset.asEntityId, None, dataset.provenance.derivedFrom.some)
      ).pure[Try]
    }
  }

  private lazy val infoFinder = new DatasetInfoFinderImpl[Try]()
}
