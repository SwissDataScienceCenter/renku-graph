/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.blankStrings
import ch.datascience.graph.model.users.{Email, Name}
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CreatorsFinderSpec extends WordSpec with ScalaCheckPropertyChecks {

  import CreatorsFinder._

  "dataset creator decoder" should {

    "decode result-set with a blank affiliation to a DatasetCreator object" in {
      forAll(emails, names, blankStrings()) { (email, name, affiliation) =>
        resultSet(email, name, affiliation).as[List[DatasetCreator]] shouldBe Right {
          List(DatasetCreator(Some(email), name, None))
        }
      }
    }

    "decode result-set with a non-blank affiliation to a DatasetCreator object" in {
      forAll(emails, names, affiliations) { (email, name, affiliation) =>
        resultSet(email, name, affiliation.toString).as[List[DatasetCreator]] shouldBe Right {
          List(DatasetCreator(Some(email), name, Some(affiliation)))
        }
      }
    }
  }

  private def resultSet(email: Email, name: Name, blank: String) = json"""
  {
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
}
