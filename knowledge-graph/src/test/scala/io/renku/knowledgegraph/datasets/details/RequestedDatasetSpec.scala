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

package io.renku.knowledgegraph.datasets.details

import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.{datasetExternalSameAs, datasetIdentifiers, datasetInternalSameAs, renkuUrls}
import io.renku.graph.model.RenkuUrl
import org.http4s.Uri.Path.SegmentEncoder
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, OptionValues}

class RequestedDatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with EitherValues
    with TableDrivenPropertyChecks {

  private implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne

  private val idTypeScenarios = Table(
    "id type"         -> "id",
    "Identifier"      -> RequestedDataset(datasetIdentifiers.generateOne),
    "Internal sameAs" -> RequestedDataset(datasetInternalSameAs.generateOne),
    "External sameAs" -> RequestedDataset(datasetExternalSameAs.generateOne)
  )

  "url codec" should {

    forAll(idTypeScenarios) { (idType, id) =>
      s"url encode and decode Dataset identifier of $idType type" in {

        val encoded = implicitly[SegmentEncoder[RequestedDataset]].toSegment(id)

        RequestedDataset.unapply(encoded.encoded).value shouldBe id
      }
    }
  }

  "json codec" should {

    forAll(idTypeScenarios) { (idType, id) =>
      s"json encode and decode Dataset identifier of $idType type" in {
        id.asJson.hcursor.as[RequestedDataset].value shouldBe id
      }
    }
  }
}
