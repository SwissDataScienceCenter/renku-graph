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

package io.renku.entities.searchgraphs

import io.renku.entities.searchgraphs.Link.{ImportedDataset, OriginalDataset}
import io.renku.entities.searchgraphs.SearchInfo.DateModified
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{datasetCreatedDates, datasetExternalSameAs, datasetResourceIds, projectPaths, projectResourceIds}
import io.renku.graph.model.datasets
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchInfoSpec extends AnyWordSpec with should.Matchers {

  "DateModified.apply(DateCreated)" should {

    "instantiate DateModified from a Dataset DateCreated" in {
      val created = datasetCreatedDates().generateOne
      DateModified(created).value shouldBe created.instant
    }
  }
}

class LinkSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "return OriginalDataset if both the topmostSameAs and datasetId are equal" in {
      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasets.TopmostSameAs(datasetId.asEntityId)
      val projectId     = projectResourceIds.generateOne
      val projectPath   = projectPaths.generateOne

      val link = Link(topmostSameAs, datasetId, projectId, projectPath)

      link            shouldBe a[OriginalDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectPath)
    }

    "return ImportedDataset if the topmostSameAs and datasetId are not equal" in {
      val datasetId     = datasetResourceIds.generateOne
      val topmostSameAs = datasetExternalSameAs.generateAs(datasets.TopmostSameAs(_))
      val projectId     = projectResourceIds.generateOne
      val projectPath   = projectPaths.generateOne

      val link = Link(topmostSameAs, datasetId, projectId, projectPath)

      link            shouldBe a[ImportedDataset]
      link.resourceId shouldBe links.ResourceId.from(topmostSameAs, projectPath)
    }

    // link sparql encoder
  }
}
