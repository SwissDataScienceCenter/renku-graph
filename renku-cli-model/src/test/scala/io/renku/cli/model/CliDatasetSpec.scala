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

package io.renku.cli.model

import io.circe.DecodingFailure
import io.renku.cli.model.Ontologies.Schema
import io.renku.cli.model.diffx.CliDiffInstances
import io.renku.cli.model.generators.DatasetGenerators
import io.renku.cli.model.tools.JsonLDTools
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CliDatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with CliDiffInstances
    with JsonLDCodecMatchers {

  private implicit val renkuUrl: RenkuUrl = RenkuTinyTypeGenerators.renkuUrls.generateOne
  private val datasetGen = DatasetGenerators.datasetGen

  "decode/encode" should {
    "be compatible" in {
      forAll(datasetGen) { cliDataset =>
        assertCompatibleCodec(cliDataset)
      }
    }

    "be compatible with publication events" in {
      forAll(DatasetGenerators.datasetWithEventsGen) { cliDataset =>
        implicit val jsonEncoder = CliDataset.flatJsonLDEncoder
        assertCompatibleCodec(cliDataset)
      }
    }

    "work on multiple items" in {
      forAll(datasetGen, datasetGen) { (cliDataset1, cliDataset2) =>
        assertCompatibleCodec(cliDataset1, cliDataset2)
      }
    }

    "work on multiple items with publication events" in {
      forAll(DatasetGenerators.datasetWithEventsGen, DatasetGenerators.datasetWithEventsGen) {
        (cliDataset1, cliDataset2) =>
          implicit val jsonEncoder = CliDataset.flatJsonLDEncoder
          assertCompatibleCodec(cliDataset1, cliDataset2)
      }
    }

    "skip publicationEvents that belong to a different dataset" in {
      val dataset                    = DatasetGenerators.datasetWithEventsGen.generateOne
      val additionalEvents           = DatasetGenerators.datasetWithEventsGen.generateOne.publicationEvents
      val datasetWithUnrelatedEvents = CliDataset.Lenses.publicationEventList.modify(additionalEvents ::: _)(dataset)

      implicit val jsonEncoder = CliDataset.flatJsonLDEncoder
      assertCompatibleCodec((_: CliDataset) => List(dataset))(datasetWithUnrelatedEvents)
    }

    "fail if no creators" in {
      val dataset = datasetGen.generateOne
      val noCreatorsJsonLD =
        JsonLDTools
          .view(dataset.asNestedJsonLD)
          .remove(CliDataset.entityTypes, Schema.creator)
          .value

      val Left(error) = noCreatorsJsonLD.cursor.as[List[CliDataset]]
      error            shouldBe a[DecodingFailure]
      error.getMessage() should include(s"No creators on dataset with id: ${dataset.identifier.value}")
    }
  }
}
