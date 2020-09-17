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

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.{fusekiBaseUrls, jsonLDTriples}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{IdSameAs, UrlSameAs}
import ch.datascience.rdfstore.entities.DataSet
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{FusekiBaseUrl, JsonLDTriples}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class DataSetInfoFinderSpec extends AnyWordSpec with should.Matchers {

  private implicit val fusekiBaseUrl: FusekiBaseUrl = fusekiBaseUrls.generateOne

  "findDatasetsInfo" should {

    "return empty set if there's no Dataset entity in the Json" in new TestCase {
      infoFinder.findDatasetsInfo(jsonLDTriples.generateOne) shouldBe Success(Set.empty)
    }

    "return the DataSetInfo without sameAs and derivedFrom when they are not present in the json" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      val entityId   = DataSet.entityId(identifier)(renkuBaseUrl)
      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = None).toJson
      }

      infoFinder.findDatasetsInfo(triples) shouldBe Success(Set((entityId, None, None)))
    }

    "return the DataSetInfo with UrlSameAs and no derivedFrom as reflected in json" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      val entityId   = DataSet.entityId(identifier)(renkuBaseUrl)
      val sameAs     = datasetUrlSameAs.generateSome
      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = sameAs).toJson
      }

      val Success(datasetInfos) = infoFinder.findDatasetsInfo(triples)
      datasetInfos             shouldBe (Set((entityId, sameAs, None)))
      datasetInfos.head._2.get shouldBe an[UrlSameAs]
    }

    "return the DataSetInfo with IdSameAs and no derivedFrom as reflected in json" in new TestCase {
      val identifier = datasetIdentifiers.generateOne
      val entityId   = DataSet.entityId(identifier)(renkuBaseUrl)
      val sameAs     = datasetIdSameAs.generateSome
      val triples = JsonLDTriples {
        nonModifiedDataSetCommit()()(datasetIdentifier = identifier, maybeDatasetSameAs = sameAs).toJson
      }

      val Success(datasetInfos) = infoFinder.findDatasetsInfo(triples)
      datasetInfos             shouldBe (Set((entityId, sameAs, None)))
      datasetInfos.head._2.get shouldBe an[IdSameAs]
    }

    "return the DataSetInfo with derivedFrom and no sameAs as reflected in json" in new TestCase {
      val identifier  = datasetIdentifiers.generateOne
      val entityId    = DataSet.entityId(identifier)(renkuBaseUrl)
      val derivedFrom = datasetDerivedFroms.generateOne
      val triples = JsonLDTriples {
        modifiedDataSetCommit()()(datasetIdentifier = identifier, datasetDerivedFrom = derivedFrom).toJson
      }

      infoFinder.findDatasetsInfo(triples) shouldBe Success(Set((entityId, None, derivedFrom.some)))
    }
  }

  private trait TestCase {
    val infoFinder = new DataSetInfoFinderImpl[Try]()
  }
}
