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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import Generators._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.RenkuTinyTypeGenerators.{imageUris, projectDescriptions, projectKeywords, projectModifiedDates, projectNames}
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects
import org.scalacheck.Gen
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class NewValuesCalculatorSpec extends AnyWordSpec with should.Matchers with OptionValues {

  "new name" should {

    "be None if ts and gl names are the same" in {

      val tsData = tsDataExtracts().generateOne
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be gl name if ts and gl contains different names" in {

      val tsData = tsDataExtracts().generateOne
      val glData = glDataFrom(tsData).copy(name = projectNames.generateOne)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeName = glData.name.some)
    }
  }

  "new visibility" should {

    "be None if ts and gl visibilities are the same" in {

      val tsData = tsDataExtracts().generateOne
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be gl visibility if ts and gl contains different values" in {

      val tsData = tsDataExtracts().generateOne
      val glData =
        glDataFrom(tsData)
          .copy(visibility = Gen.oneOf(projects.Visibility.all - tsData.visibility).generateOne)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeVisibility = glData.visibility.some)
    }
  }

  "new dateModified" should {

    "be None if ts and gl values are the same" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDateModified = projectModifiedDates().generateSome)
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be None if ts and gl values are not set - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDateModified = None)
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be None if ts and gl values are the same, irrespectively to the payload value" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDateModified = projectModifiedDates().generateOption)
      val glData      = glDataFrom(tsData)
      val payloadData = payloadDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe NewValues.empty
    }

    "be None if gl date <= ts date" in {

      val tsDate = projectModifiedDates().generateOne
      val tsData = tsDataExtracts().generateOne.copy(maybeDateModified = tsDate.some)
      val glData = glDataFrom(tsData)
        .copy(updatedAt = timestamps(max = tsDate.value).generateOne.some)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be gl value if gl date > ts date" in {

      val tsDate = projectModifiedDates().generateOne
      val tsData = tsDataExtracts().generateOne.copy(maybeDateModified = tsDate.some)
      val glData =
        glDataFrom(tsData)
          .copy(updatedAt = projectModifiedDates(tsDate.value.plusSeconds(1)).generateSome.map(_.value))

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeDateModified = glData.maybeDateModified)
    }

    "be None if ts is set but gl not" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDateModified = projectModifiedDates().generateSome)
      val glData = glDataFrom(tsData).copy(updatedAt = None, lastActivityAt = None)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be gl desc if ts is not set but gl is" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDateModified = None)
      val glData = glDataFrom(tsData).copy(
        updatedAt = projectModifiedDates().generateSome.map(_.value),
        lastActivityAt = None
      )

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeDateModified = glData.maybeDateModified)
    }
  }

  "new description" should {

    "be None if ts and gl descriptions are set the same - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be None if ts and payload descriptions are set the same" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData      = glDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)
      val payloadData = payloadDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe NewValues.empty
    }

    "be None if ts and gl descriptions are not set - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDesc = None)
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be None if ts, gl and payload descriptions are not set" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = None)
      val glData      = glDataFrom(tsData)
      val payloadData = payloadDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe NewValues.empty
    }

    "be gl desc if ts and gl contains different values - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData = glDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeDesc = Some(glData.maybeDesc))
    }

    "be payload desc if ts and payload contains different values" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData      = glDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)
      val payloadData = payloadDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeDesc = Some(payloadData.maybeDesc))
    }

    "be reset to None if ts is set but gl not - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData = glDataFrom(tsData).copy(maybeDesc = None)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeDesc = Some(None))
    }

    "be reset to None if ts is set but gl and payload not" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData      = glDataFrom(tsData).copy(maybeDesc = None)
      val payloadData = payloadDataFrom(tsData).copy(maybeDesc = None)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeDesc = Some(None))
    }

    "be gl desc if ts is not set but gl is - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(maybeDesc = None)
      val glData = glDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeDesc = Some(glData.maybeDesc))
    }

    "be payload desc if all ts, gl and payload have different values" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateOption)
      val glData      = glDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateOption)
      val payloadData = payloadDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeDesc = Some(payloadData.maybeDesc))
    }

    "be payload desc if ts and payload have different values and there's no desc in gl" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateOption)
      val glData      = glDataFrom(tsData).copy(maybeDesc = None)
      val payloadData = payloadDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeDesc = Some(payloadData.maybeDesc))
    }

    "be gl desc if ts and gl have different values - no payload case" in {

      val tsData      = tsDataExtracts().generateOne.copy(maybeDesc = projectDescriptions.generateSome)
      val glData      = glDataFrom(tsData).copy(maybeDesc = projectDescriptions.generateSome)
      val payloadData = payloadDataFrom(tsData).copy(maybeDesc = None)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeDesc = Some(glData.maybeDesc))
    }
  }

  "new keywords" should {

    "be None if ts and gl values are the same - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(keywords = projectKeywords.generateSet())
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be None if ts and payload values are the same" in {

      val tsData      = tsDataExtracts().generateOne.copy(keywords = projectKeywords.generateSet(min = 1))
      val glData      = glDataFrom(tsData).copy(keywords = projectKeywords.generateSet(min = 1))
      val payloadData = payloadDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe NewValues.empty
    }

    "be gl keywords if ts and gl contains different values - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(keywords = projectKeywords.generateSet())
      val glData = glDataFrom(tsData).copy(keywords = projectKeywords.generateSet(min = 1))

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeKeywords = glData.keywords.some)
    }

    "be gl keywords if ts and gl contains different values - no keywords in the payload" in {

      val tsData      = tsDataExtracts().generateOne.copy(keywords = projectKeywords.generateSet())
      val glData      = glDataFrom(tsData).copy(keywords = projectKeywords.generateSet(min = 1))
      val payloadData = payloadDataFrom(tsData).copy(keywords = Set.empty)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeKeywords = glData.keywords.some)
    }

    "be payload keywords if ts and payload contains different values" in {

      val tsData      = tsDataExtracts().generateOne.copy(keywords = projectKeywords.generateSet())
      val glData      = glDataFrom(tsData).copy(keywords = projectKeywords.generateSet(min = 1))
      val payloadData = payloadDataFrom(tsData).copy(keywords = projectKeywords.generateSet(min = 1))

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeKeywords = payloadData.keywords.some)
    }
  }

  "new images" should {

    "be None if ts and gl values are the same - no payload case" in {

      val tsData = tsDataExtracts().generateOne.copy(images = imageUris.generateFixedSizeList(ofSize = 1))
      val glData = glDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe NewValues.empty
    }

    "be None if ts and payload values are the same" in {

      val tsData      = tsDataExtracts().generateOne.copy(images = imageUris.generateFixedSizeList(ofSize = 1))
      val glData      = glDataFrom(tsData).copy(maybeImage = imageUris.generateSome)
      val payloadData = payloadDataFrom(tsData)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe NewValues.empty
    }

    "be gl image if ts and gl contains different values - no payload case" in {

      val tsData  = tsDataExtracts().generateOne
      val glImage = imageUris.generateOne
      val glData  = glDataFrom(tsData).copy(maybeImage = glImage.some)

      NewValuesCalculator.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        NewValues.empty.copy(maybeImages = List(Image.projectImage(tsData.id, glImage)).some)
    }

    "be gl image if ts and gl contains different values - no images in the payload" in {

      val tsData      = tsDataExtracts().generateOne
      val glImage     = imageUris.generateOne
      val glData      = glDataFrom(tsData).copy(maybeImage = glImage.some)
      val payloadData = payloadDataFrom(tsData).copy(images = List.empty)

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeImages = List(Image.projectImage(tsData.id, glImage)).some)
    }

    "be payload images if ts and payload contains different values" in {

      val tsData      = tsDataExtracts().generateOne
      val glData      = glDataFrom(tsData).copy(maybeImage = imageUris.generateSome)
      val payloadData = payloadDataFrom(tsData).copy(images = imageUris.generateFixedSizeList(ofSize = 1))

      NewValuesCalculator.findNewValues(tsData, glData, payloadData.some) shouldBe
        NewValues.empty.copy(maybeImages = Image.projectImage(tsData.id, payloadData.images).some)
    }
  }
}
