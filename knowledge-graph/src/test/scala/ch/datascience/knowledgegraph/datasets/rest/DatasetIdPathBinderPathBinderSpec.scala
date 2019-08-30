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

package ch.datascience.knowledgegraph.datasets.rest

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class DatasetIdPathBinderPathBinderSpec extends WordSpec {

  "unapply" should {

    "convert valid dataset id as string to Identifier" in {
      val id = datasetIds.generateOne
      DatasetIdPathBinder.unapply(id.toString) shouldBe Some(id)
    }

    "return None if string value cannot be converted to in Identifier" in {
      DatasetIdPathBinder.unapply("a") shouldBe None
    }

    "return None if string value is blank" in {
      DatasetIdPathBinder.unapply(" ") shouldBe None
    }
  }
}
