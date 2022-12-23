/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.data.Kleisli
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchInfoExtractorSpec extends AnyWordSpec with should.Matchers {

  "extractSearchInfo" should {

    "convert the given Datasets to SearchInfo objects" in {

      val project = anyRenkuProjectEntities.generateOne.to[entities.Project]

      val modifiedDatasets = Kleisli(datasetEntities(provenanceNonModified))
        .flatMap(ds => Kleisli(ds.createModification()))
        .run(project.dateCreated)
        .generateList()

      val nonModifiedDatasets =
        Kleisli(datasetEntities(provenanceNonModified)).run(project.dateCreated).generateList(max = 10)

      val datasets = (modifiedDatasets ::: nonModifiedDatasets)
        .map(_.to[entities.Dataset[entities.Dataset.Provenance]])

      SearchInfoExtractor.extractSearchInfo(project)(datasets) shouldBe datasets.map { ds =>
        SearchInfo(
          ds.resourceId,
          ds.identification.name,
          project.visibility,
          ds.provenance.date,
          ds.provenance.creators,
          ds.additionalInfo.keywords,
          ds.additionalInfo.maybeDescription,
          ds.additionalInfo.images,
          List(project.resourceId)
        )
      }
    }
  }
}
