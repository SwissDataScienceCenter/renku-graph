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

package io.renku.entities.searchgraphs.datasets

import Generators.linkObjectsGen
import Link.{ImportedDataset, OriginalDataset}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{datasetExternalSameAs, datasetResourceIds, datasetTopmostSameAs, projectResourceIds, projectSlugs, projectVisibilities}
import io.renku.graph.model.datasets
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class LinkSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "apply/from" should {

    "return OriginalDataset if both the topmostSameAs and datasetId are equal" in {

      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasets.TopmostSameAs(datasetId.asEntityId)
      val projectId     = projectResourceIds.generateOne
      val projectSlug   = projectSlugs.generateOne
      val visibility    = projectVisibilities.generateOne

      val link = Link.from(topmostSameAs, datasetId, projectId, projectSlug, visibility)

      link            shouldBe a[OriginalDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectSlug)
    }

    "return OriginalDataset if linkId starts with datasetId" in {

      val datasetId   = datasetResourceIds.generateOne
      val projectId   = projectResourceIds.generateOne
      val projectSlug = projectSlugs.generateOne
      val linkId      = links.ResourceId.from(datasets.TopmostSameAs(datasetId.asEntityId), projectSlug)
      val visibility  = projectVisibilities.generateOne

      val link = Link(linkId, datasetId, projectId, projectSlug, visibility)

      link             shouldBe a[OriginalDataset]
      link.resourceId  shouldBe linkId
      link.projectId   shouldBe projectId
      link.datasetId   shouldBe datasetId
      link.projectSlug shouldBe projectSlug
      link.visibility  shouldBe visibility
    }

    "return ImportedDataset if the topmostSameAs and datasetId are not equal" in {

      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_))
      val projectId     = projectResourceIds.generateOne
      val projectSlug   = projectSlugs.generateOne
      val visibility    = projectVisibilities.generateOne

      val link = Link.from(topmostSameAs, datasetId, projectId, projectSlug, visibility)

      link            shouldBe a[ImportedDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectSlug)
    }

    "return ImportedDataset if linkId does not starts with datasetId" in {

      val datasetId   = datasetResourceIds.generateOne
      val projectId   = projectResourceIds.generateOne
      val projectSlug = projectSlugs.generateOne
      val linkId      = links.ResourceId.from(datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_)), projectSlug)
      val visibility  = projectVisibilities.generateOne

      val link = Link(linkId, datasetId, projectId, projectSlug, visibility)

      link             shouldBe a[ImportedDataset]
      link.resourceId  shouldBe linkId
      link.projectId   shouldBe projectId
      link.datasetId   shouldBe datasetId
      link.projectSlug shouldBe projectSlug
      link.visibility  shouldBe visibility
    }
  }

  "show" should {

    "returns String representation" in {
      forAll(linkObjectsGen(datasetTopmostSameAs.generateOne)) { link =>
        link.show shouldBe
          show"id = ${link.resourceId}, projectId = ${link.projectId}, datasetId = ${link.datasetId}, visibility = ${link.visibility}"
      }
    }
  }
}
