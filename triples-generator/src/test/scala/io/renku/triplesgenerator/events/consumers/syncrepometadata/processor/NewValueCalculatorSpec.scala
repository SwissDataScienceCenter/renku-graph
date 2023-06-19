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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import Generators._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectNames
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class NewValueCalculatorSpec extends AnyWordSpec with should.Matchers with OptionValues {

  "new name" should {

    "be None if ts and gl names are the same - no payload case" in {

      val tsData = tsDataExtracts().generateOne
      val glData = glDataFrom(tsData)

      NewValueCalculatord.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        newValuesFrom(tsData).copy(maybeName = None)
    }

    "be None if ts and payload names are the same" in {

      val tsData      = tsDataExtracts().generateOne
      val glData      = glDataFrom(tsData).copy(name = projectNames.generateOne)
      val payloadData = payloadDataFrom(tsData)

      NewValueCalculatord.findNewValues(tsData, glData, payloadData.some) shouldBe
        newValuesFrom(tsData).copy(maybeName = None)
    }

    "be gl name if ts and gl contains different names - no payload case" in {

      val tsData = tsDataExtracts().generateOne
      val glData = glDataFrom(tsData).copy(name = projectNames.generateOne)

      NewValueCalculatord.findNewValues(tsData, glData, maybePayloadData = None) shouldBe
        newValuesFrom(tsData).copy(maybeName = glData.name.some)
    }

    "be payload name if all ts, gl and payload contains different names" in {

      val tsData      = tsDataExtracts().generateOne
      val glData      = glDataFrom(tsData).copy(name = projectNames.generateOne)
      val payloadData = payloadDataFrom(tsData).copy(name = projectNames.generateOne)

      NewValueCalculatord.findNewValues(tsData, glData, payloadData.some) shouldBe
        newValuesFrom(tsData).copy(maybeName = payloadData.name.some)
    }
  }
}
