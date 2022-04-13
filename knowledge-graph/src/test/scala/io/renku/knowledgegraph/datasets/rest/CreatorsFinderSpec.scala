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

package io.renku.knowledgegraph.datasets.rest

import cats.data.NonEmptyList
import io.circe.Decoder
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.blankStrings
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.persons.{Email, Name}
import io.renku.knowledgegraph.datasets.model.DatasetCreator
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CreatorsFinderSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  import CreatorsFinder._

  "dataset creator decoder" should {

    "decode result-set with a blank affiliation to a DatasetCreator object" in {
      forAll(personEmails, personNames, blankStrings()) { (email, name, affiliation) =>
        resultSet(email, name, affiliation).as[NonEmptyList[DatasetCreator]] shouldBe Right {
          NonEmptyList.of(DatasetCreator(Some(email), name, None))
        }
      }
    }

    "decode result-set with a non-blank affiliation to a DatasetCreator object" in {
      forAll(personEmails, personNames, personAffiliations) { (email, name, affiliation) =>
        resultSet(email, name, affiliation.toString).as[NonEmptyList[DatasetCreator]] shouldBe Right {
          NonEmptyList.of(DatasetCreator(Some(email), name, Some(affiliation)))
        }
      }
    }
  }

  private def resultSet(email: Email, name: Name, blank: String) = json"""{
    "results": {
      "bindings": [
        {
          "email": {"value": ${email.value}},
          "name": {"value": ${name.value}},
          "affiliation": {"value": $blank}
        }
      ]
    }
  }"""

  private implicit lazy val decoder: Decoder[NonEmptyList[DatasetCreator]] =
    creatorsDecoder(datasetIdentifiers.generateOne)
}
