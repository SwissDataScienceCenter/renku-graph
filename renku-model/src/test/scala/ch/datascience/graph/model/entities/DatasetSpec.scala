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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{timestamps, timestampsNotInTheFuture}
import ch.datascience.graph.model._
import ch.datascience.graph.model.entities.Dataset.Provenance
import ch.datascience.graph.model.testentities._
import io.circe.DecodingFailure
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "Dataset.decode" should {

    "turn JsonLD Dataset entity into the Dataset object" in {
      forAll(datasetEntities(ofAnyProvenance)) { dataset =>
        dataset.asJsonLD.cursor.as[entities.Dataset[entities.Dataset.Provenance]] shouldBe dataset
          .to[entities.Dataset[entities.Dataset.Provenance]]
          .asRight
      }
    }

    "fail if dataset parts are older than the internal or imported external dataset" in {
      List(
        datasetEntities(datasetProvenanceInternal),
        datasetEntities(datasetProvenanceImportedExternal),
        datasetEntities(datasetProvenanceImportedInternalAncestorExternal)
      ).foreach { datasetGen =>
        val dataset = datasetGen.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
        val invalidPart = updatePartDateAfter(
          datasetPartEntities(timestampsNotInTheFuture.generateOne).generateOne
            .to[entities.DatasetPart]
        )(dataset.provenance)
        val invalidDataset = dataset.copy(parts = invalidPart :: dataset.parts)

        val Left(error) = invalidDataset.asJsonLD.cursor.as[entities.Dataset[entities.Dataset.Provenance]]
        error shouldBe a[DecodingFailure]
        error.getMessage shouldBe s"Dataset ${invalidDataset.identification.identifier} " +
          s"Part ${invalidPart.entity.location} startTime ${invalidPart.dateCreated} is older than Dataset ${invalidDataset.provenance.date.instant}"
      }
    }

    "succeed if dataset parts are older than the modified or imported internal dataset" in {
      List(
        datasetEntities(datasetProvenanceModified),
        datasetEntities(datasetProvenanceImportedInternalAncestorInternal)
      ).foreach { datasetGen =>
        val dataset = datasetGen.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
        val olderPart = updatePartDateAfter(
          datasetPartEntities(timestampsNotInTheFuture.generateOne).generateOne
            .to[entities.DatasetPart]
        )(dataset.provenance)
        val validDataset = dataset.copy(parts = olderPart :: dataset.parts)

        validDataset.asJsonLD.cursor.as[entities.Dataset[entities.Dataset.Provenance]] shouldBe Right(validDataset)
      }
    }

    "fail if invalidationTime is older than the dataset" in {
      val dataset          = datasetEntities(ofAnyProvenance).generateOne
      val invalidationTime = timestamps(max = dataset.provenance.date.instant).generateAs(InvalidationTime)
      val invalidatedDataset = dataset
        .to[entities.Dataset[entities.Dataset.Provenance]]
        .copy(maybeInvalidationTime = invalidationTime.some)

      val Left(error) = invalidatedDataset.asJsonLD.cursor.as[entities.Dataset[entities.Dataset.Provenance]]
      error shouldBe a[DecodingFailure]
      error.getMessage shouldBe s"Dataset ${invalidatedDataset.identification.identifier} " +
        s"invalidationTime $invalidationTime is older than Dataset ${invalidatedDataset.provenance.date}"
    }
  }

  private def updatePartDateAfter(
      part: entities.DatasetPart
  ): entities.Dataset.Provenance => entities.DatasetPart = {
    case p: Provenance.Modified =>
      part.copy(dateCreated = timestamps(max = p.date.value).generateAs[datasets.DateCreated])
    case p: Provenance.Internal =>
      part.copy(dateCreated = timestamps(max = p.date.value).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedExternal =>
      part.copy(dateCreated = timestamps(max = p.date.instant).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedInternalAncestorExternal =>
      part.copy(dateCreated = timestamps(max = p.date.instant).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedInternalAncestorInternal =>
      part.copy(dateCreated = timestamps(max = p.date.value).generateAs[datasets.DateCreated])
  }
}
