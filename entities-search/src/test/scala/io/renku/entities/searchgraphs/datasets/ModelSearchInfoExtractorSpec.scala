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

package io.renku.entities.searchgraphs.datasets

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, entities}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class ModelSearchInfoExtractorSpec extends AnyFlatSpec with should.Matchers {

  it should "convert the given non-modified Datasets to SearchInfo objects" in {

    val project = anyRenkuProjectEntities
      .withDatasets(List.fill(positiveInts(max = 5).generateOne.value)(datasetEntities(provenanceNonModified)): _*)
      .generateOne
      .to[entities.Project]

    val datasets = project.datasets

    ModelSearchInfoExtractor.extractModelSearchInfos[Try](project)(datasets) shouldBe datasets
      .map { ds =>
        ModelDatasetSearchInfo(
          ds.provenance.topmostSameAs,
          ds.identification.name,
          ds.identification.slug,
          ds.provenance.date,
          maybeDateModified = None,
          ds.provenance.creators.map(Creator.from),
          ds.additionalInfo.keywords,
          ds.additionalInfo.maybeDescription,
          ds.additionalInfo.images,
          Link.from(ds.provenance.topmostSameAs, ds.resourceId, project.resourceId, project.slug, project.visibility)
        )
      }
      .pure[Try]
  }

  it should "convert the given modified Datasets to SearchInfo objects" in {

    val project = anyRenkuProjectEntities
      .addDatasetAndModifications(datasetEntities(provenanceNonModified), level = 2)
      .generateOne
      .to[entities.Project]

    val originalDataset  = project.datasets.head
    val lastModification = project.datasets.last

    ModelSearchInfoExtractor.extractModelSearchInfos[Try](project)(List(lastModification)) shouldBe List(
      ModelDatasetSearchInfo(
        lastModification.provenance.topmostSameAs,
        lastModification.identification.name,
        lastModification.identification.slug,
        originalDataset.provenance.date,
        datasets.DateModified(lastModification.provenance.date).some,
        lastModification.provenance.creators.map(Creator.from),
        lastModification.additionalInfo.keywords,
        lastModification.additionalInfo.maybeDescription,
        lastModification.additionalInfo.images,
        Link.from(lastModification.provenance.topmostSameAs,
                  lastModification.resourceId,
                  project.resourceId,
                  project.slug,
                  project.visibility
        )
      )
    ).pure[Try]
  }
}
