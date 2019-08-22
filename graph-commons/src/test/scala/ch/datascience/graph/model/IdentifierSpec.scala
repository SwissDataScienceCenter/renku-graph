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

package ch.datascience.graph.model

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.dataSets.Identifier
import org.scalacheck.Gen._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class IdentifierSpec extends WordSpec with ScalaCheckPropertyChecks {

  "from" should {

    "return Identifier for valid uuid" in {
      forAll(uuid) { expected =>
        val Right(Identifier(actual)) = Identifier.from(expected.toString)
        actual shouldBe expected.toString
      }
    }

    "fail for invalid value" in {

      val invalidUuid = nonEmptyStrings().generateOne

      val Left(exception) = Identifier.from(invalidUuid)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe s"Cannot instantiate ${Identifier.typeName} with '$invalidUuid'"
    }
  }
}
