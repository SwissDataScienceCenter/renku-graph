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

package io.renku.triplesgenerator.api.events

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import Generators._
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.datasets
import io.renku.graph.model.RenkuTinyTypeGenerators.{datasetIdentifiers, datasetViewedDates}
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class DatasetViewedEventSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues
    with MockFactory {

  "forDataset" should {

    "instantiate a new event with the current timestamp" in {

      val currentTime = Instant.now()
      val now         = mockFunction[Instant]
      now.expects().returning(currentTime)

      val identifier = datasetIdentifiers.generateOne

      DatasetViewedEvent.forDataset(identifier, now) shouldBe DatasetViewedEvent(identifier, currentTime)
    }
  }

  "json codec" should {

    "encode and decode" in {

      val event = datasetViewedEvents.generateOne

      event.asJson.hcursor.as[DatasetViewedEvent].value shouldBe event
    }

    "be able to decode json valid from the contract point of view" in {

      json"""{
        "categoryName": "DATASET_VIEWED",
        "dataset": {
          "identifier": "12345"
        },
        "date": "1988-11-04T00:00:00.000Z"
      }""".hcursor.as[DatasetViewedEvent].value shouldBe DatasetViewedEvent(
        datasets.Identifier("12345"),
        datasets.DateViewed(Instant.parse("1988-11-04T00:00:00.000Z"))
      )
    }

    "fail if categoryName does not match" in {

      val otherCategory = nonEmptyStrings().generateOne
      val result = json"""{
        "categoryName": $otherCategory,
        "dataset": {
          "identifier": ${datasetIdentifiers.generateOne}
        },
        "date": ${datasetViewedDates().generateOne}
      }""".hcursor.as[DatasetViewedEvent]

      result.left.value.getMessage() should include(s"Expected DATASET_VIEWED but got $otherCategory")
    }
  }

  "show" should {

    "return String info with path and the date" in {

      val event = datasetViewedEvents.generateOne

      event.show shouldBe show"datasetIdentifier = ${event.identifier}, date = ${event.dateViewed}"
    }
  }
}