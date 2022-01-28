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

package io.renku.knowledgegraph.graphql

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import sangria.ast.StringValue
import sangria.schema.ScalarType
import sangria.validation.ValueCoercionViolation

class ArgumentsSpec extends AnyWordSpec with should.Matchers {

  import Arguments._

  "TinyTypeOps" should {

    "provide an extension method toScalarType which converts a String TinyType to ScalarType" in {
      val name:        NonBlank = Refined.unsafeApply(nonEmptyStrings().generateOne)
      val description: NonBlank = Refined.unsafeApply(nonEmptyStrings().generateOne)

      val scalarType = TestTinyType.toScalarType(name, description)

      scalarType             shouldBe a[ScalarType[_]]
      scalarType.name        shouldBe name.value
      scalarType.description shouldBe Some(description.value)
    }

    "provide an extension method toScalarType which returns a ScalarType converting user value to the TinyType" in {
      val scalarType = TestTinyType.toScalarType("name", "description")

      val value = nonEmptyStrings().generateOne
      scalarType.coerceUserInput(value) shouldBe Right(TestTinyType(value))
      val Left(exception) = scalarType.coerceUserInput(positiveInts().generateOne)
      exception              shouldBe a[ValueCoercionViolation]
      exception.errorMessage shouldBe "name has invalid value"
    }

    "provide an extension method toScalarType which returns a ScalarType converting the AST value to the TinyType" in {
      val scalarType = TestTinyType.toScalarType("name", "description")

      val value = nonEmptyStrings().generateOne
      scalarType.coerceInput(StringValue(value)) shouldBe Right(TestTinyType(value))
      val Left(exception) = scalarType.coerceUserInput(positiveInts().generateOne)
      exception              shouldBe a[ValueCoercionViolation]
      exception.errorMessage shouldBe "name has invalid value"
    }

    "provide an extension method toScalarType which returns a ScalarType which uses the given exception message on value conversion failures" in {
      val customConversionMessage: NonBlank = Refined.unsafeApply(nonEmptyStrings().generateOne)

      val scalarType = TestTinyType.toScalarType("name", "description", customConversionMessage)

      val Left(exception) = scalarType.coerceUserInput(positiveInts().generateOne)
      exception.errorMessage shouldBe customConversionMessage.value
    }
  }

  type NonBlank = String Refined NonEmpty
  case class TestTinyType(value: String) extends StringTinyType
  object TestTinyType                    extends TinyTypeFactory[TestTinyType](new TestTinyType(_))
}
