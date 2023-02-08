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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.{CliDataset, CliDatasetFile}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.testentities._
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.{InvalidationTime, entities}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetPartSpec
    extends AnyWordSpec
    with should.Matchers
    with EitherValues
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with DiffInstances {

  "decode" should {
    "turn JsonLD DatasetPart entity into the DatasetPart object" in {
      val startDate = timestampsNotInTheFuture.generateOne
      forAll(datasetPartEntities(startDate)) { datasetPart =>
        datasetPart
          .to[CliDatasetFile]
          .asFlattenedJsonLD
          .cursor
          .as[List[entities.DatasetPart]] shouldMatchToRight List(datasetPart.to[entities.DatasetPart])
      }
    }

    "turn JsonLD DatasetPart with InvalidationTime entity into the DatasetPart object" in {
      forAll(datasetEntities(provenanceNonModified(cliShapedPersons)).decoupledFromProject) { dataset =>
        val datasetPart      = datasetPartEntities(dataset.provenance.date.instant).generateOne
        val invalidationTime = invalidationTimes(datasetPart.dateCreated.value).generateOne
        val invalidatedDataset = dataset
          .copy(parts = List(datasetPart))
          .invalidatePart(datasetPart, invalidationTime, cliShapedPersons)
          .fold(errors => fail(errors.intercalate("; ")), identity)

        invalidatedDataset
          .to[CliDataset]
          .asFlattenedJsonLD
          .cursor
          .as[List[entities.DatasetPart]] shouldBe invalidatedDataset.parts.map(_.to[entities.DatasetPart]).asRight
      }
    }

    "fail if invalidationTime is older than the part" in {
      val datasetPart      = datasetPartEntities(timestampsNotInTheFuture.generateOne).generateOne.to[CliDatasetFile]
      val invalidationTime = timestamps(max = datasetPart.dateCreated.value).generateAs(InvalidationTime)

      val result = datasetPart
        .copy(invalidationTime = invalidationTime.some)
        .asFlattenedJsonLD
        .cursor
        .as[List[entities.DatasetPart]]

      result.left.value          shouldBe a[DecodingFailure]
      result.left.value.getMessage should include
      s"invalidationTime $invalidationTime is older than DatasetPart ${datasetPart.dateCreated.value}"
    }
  }
}
