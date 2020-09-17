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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{DerivedFrom, TopmostSameAs}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.renku.jsonld.generators.JsonLDGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class TopmostDataFinderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "findTopmostData" should {

    "return a TopmostDataInfo with sameAs and derivedFrom pointing to the dataset id " +
      "if there is no sameAs and derivedFrom in the DatasetInfo" in new TestCase {
      topmostDataFinder.findTopmostData(entityId, None, None) shouldBe TopmostData(
        entityId,
        TopmostSameAs(entityId),
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with sameAs from DatasetInfo if sameAs is pointing to a non renku url" in new TestCase {
      val sameAs = datasetUrlSameAs.generateOne

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        TopmostSameAs(sameAs),
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with parent's topmostSameAs " +
      "if sameAs is pointing to a renku dataset and there's a parent dataset" in new TestCase {
      val sameAs = datasetIdSameAs.generateOne

      val parentTopmostSameAs = datasetTopmostSameAs.generateOne

      (kgDatasetInfoFinder.findTopmostSameAs _).expects(sameAs).returning(Some(parentTopmostSameAs).pure[Try])

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        parentTopmostSameAs,
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with the given sameAs " +
      "if the parent dataset cannot be found" in new TestCase {
      val sameAs = datasetIdSameAs.generateOne

      (kgDatasetInfoFinder.findTopmostSameAs _).expects(sameAs).returning(None.pure[Try])

      topmostDataFinder.findTopmostData(entityId, Some(sameAs), None) shouldBe TopmostData(
        entityId,
        TopmostSameAs(sameAs),
        DerivedFrom(entityId)
      ).pure[Try]
    }

    "return a TopmostDataInfo with derivedFrom from the parent " +
      "if there's a derivedFrom on the parent" in new TestCase {
      val derivedFrom       = datasetDerivedFroms.generateOne
      val parentDerivedFrom = datasetDerivedFroms.generateOne

      (kgDatasetInfoFinder.findTopmostDerivedFrom _).expects(derivedFrom).returning(Some(parentDerivedFrom).pure[Try])

      topmostDataFinder.findTopmostData(entityId, None, Some(derivedFrom)) shouldBe TopmostData(
        entityId,
        TopmostSameAs(entityId),
        parentDerivedFrom
      ).pure[Try]
    }

    "return a TopmostDataInfo with the given derivedFrom " +
      "if there's no derivedFrom on the parent" in new TestCase {
      val derivedFrom = datasetDerivedFroms.generateOne

      (kgDatasetInfoFinder.findTopmostDerivedFrom _).expects(derivedFrom).returning(None.pure[Try])

      topmostDataFinder.findTopmostData(entityId, None, Some(derivedFrom)) shouldBe TopmostData(
        entityId,
        TopmostSameAs(entityId),
        derivedFrom
      ).pure[Try]
    }

    "fail if there's both sameAs and derivedFrom given" in new TestCase {
      val Failure(exception) = topmostDataFinder.findTopmostData(
        entityId,
        datasetSameAs.generateSome,
        datasetDerivedFroms.generateSome
      )

      exception            should not be a[ProcessingRecoverableError]
      exception.getMessage shouldBe s"Dataset with $entityId found in the generated triples has both sameAs and derivedFrom"
    }
  }

  private trait TestCase {
    val entityId = entityIds.generateOne

    val kgDatasetInfoFinder = mock[KGDatasetInfoFinder[Try]]
    val topmostDataFinder   = new TopmostDataFinderImpl[Try](kgDatasetInfoFinder)
  }
}
