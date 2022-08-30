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

package io.renku.knowledgegraph.datasets

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class bindersSpec extends AnyWordSpec with should.Matchers {

  "DatasetId.unapply" should {

    "convert valid dataset id as string to Identifier" in {
      val id = datasetIdentifiers.generateOne
      DatasetId.unapply(id.toString) shouldBe Some(id)
    }

    "return None if string value is blank" in {
      DatasetId.unapply(" ") shouldBe None
    }
  }

  "DatasetName.unapply" should {

    "convert valid dataset name as string to Name" in {
      val name = datasetNames.generateOne
      DatasetName.unapply(name.toString) shouldBe Some(name)
    }

    "return None if string value is blank" in {
      DatasetName.unapply(" ") shouldBe None
    }
  }
}