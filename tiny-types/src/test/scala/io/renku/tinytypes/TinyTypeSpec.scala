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

package io.renku.tinytypes

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.tinytypes.constraints.PathSegment
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

object TinyTypeData {
  val arbitraryValues: List[Any] =
    "abc" +: 2 +: 2L +: true +: List.empty[Any]
}

class TinyTypeSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "toString" should {

    "return a String value of the 'value' property" in {
      TinyTypeData.arbitraryValues foreach { someValue =>
        val tinyType: TinyType = new TinyType {
          type V = Any
          override val value: Any = someValue
        }

        tinyType.toString shouldBe someValue.toString
      }
    }
  }

  "stringTinyTypeConverter" should {

    "url encode the given value and convert it to a single element list of PathSegment" in {
      forAll(nonEmptyStrings(), Gen.oneOf("\\", " "), nonEmptyStrings()) { (part1, part2, part3) =>
        val tinyType = new StringTinyType { val value = s"$part1$part2$part3" }
        StringTinyType.stringTinyTypeConverter(tinyType) shouldBe List(PathSegment(tinyType.value))
      }
    }
  }

  "relativePathTinyTypeConverter" should {

    "do not url encode the given value and convert it to a multiple elements list of PathSegment" in {
      forAll(nonEmptyList(nonEmptyStrings())) { segments =>
        val tinyType = new RelativePathTinyType { val value = segments.toList.mkString("/") }
        RelativePathTinyType.relativePathTinyTypeConverter(tinyType) shouldBe segments.toList.map(PathSegment.apply)
      }
    }
  }
}

class SensitiveSpec extends AnyWordSpec with should.Matchers {

  "toString" should {

    "return a '<sensitive>' instead of the value" in {
      TinyTypeData.arbitraryValues foreach { someValue =>
        val tinyType: TinyType = new TinyType with Sensitive {
          type V = Any
          override val value: Any = someValue
        }

        tinyType.toString shouldBe "<sensitive>"
      }
    }
  }
}

class TinyTypeFactorySpec extends AnyWordSpec with should.Matchers {

  import TinyTypeTest._

  "apply" should {

    "instantiate object of the relevant type" in {
      TinyTypeTest("def").value shouldBe "def"
    }

    "throw an IllegalArgument exception if the first type constraint is not met" in {
      intercept[IllegalArgumentException] {
        TinyTypeTest("abc")
      }.getMessage shouldBe "io.renku.tinytypes.TinyTypeTest cannot have 'abc' value"
    }

    "throw an IllegalArgument exception if one of defined type constraints is not met" in {
      intercept[IllegalArgumentException] {
        TinyTypeTest(s"def$invalidChar")
      }.getMessage shouldBe "! is not allowed"
    }
  }

  "from" should {

    import TinyTypeTest._

    "return Right with instantiated object for valid values" in {
      TinyTypeTest.from("def").map(_.value) shouldBe Right("def")
    }

    "return Left with the IllegalArgumentException containing errors if the raw value tranformation fails" in {
      val result = TinyTypeTest.from(invalidForTransformation)

      result shouldBe a[Left[_, _]]

      val Left(exception) = result
      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe invalidTransformationException.getMessage
    }

    "return Left with the IllegalArgumentException containing errors if the type constraints are not met" in {
      val result = TinyTypeTest.from(invalidValue)

      result shouldBe a[Left[_, _]]

      val Left(exception) = result
      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe invalidValueMessage
    }
  }

  "implicit show" should {

    "return a String value of the 'value' property" in {
      TinyTypeData.arbitraryValues foreach { someValue =>
        case class InnerTinyType(v: Any) extends TinyType {
          type V = Any
          override val value: Any = v
        }

        object InnerTinyTypeFactory extends TinyTypeFactory[InnerTinyType](InnerTinyType)
        import InnerTinyTypeFactory.show

        val tinyType = InnerTinyTypeFactory(someValue)

        tinyType.show shouldBe someValue.toString
      }
    }
  }
}

class TypeNameSpec extends AnyWordSpec with should.Matchers {

  "typeName" should {

    "return type name without $ signs in case of nested types" in {
      TestTypeName.typeName shouldBe "io.renku.tinytypes.TypeNameSpec.TestTypeName"
    }

    "return type name for non-nested types" in {
      TinyTypeTest.typeName shouldBe "io.renku.tinytypes.TinyTypeTest"
    }
  }

  "shortTypeName" should {

    "return the type name without the package info and $ signs in case of nested types" in {
      TestTypeName.shortTypeName shouldBe "TestTypeName"
    }

    "return type name without the package info for non-nested types" in {
      TinyTypeTest.shortTypeName shouldBe "TinyTypeTest"
    }
  }

  private object TestTypeName extends TypeName
}

class RefinedValueSpec extends AnyWordSpec with should.Matchers {

  private class RefinedTinyTypeTest private (val value: String) extends StringTinyType
  private object RefinedTinyTypeTest
      extends TinyTypeFactory[RefinedTinyTypeTest](new RefinedTinyTypeTest(_))
      with RefinedValue[RefinedTinyTypeTest, NonEmpty]

  "RefinedValue" should {

    "add asRefined extension method on a TinyType instance for which Factory it's mixed in" in {
      val tt = nonEmptyStrings().generateAs(RefinedTinyTypeTest)

      val v: String Refined NonEmpty = tt.asRefined

      v.value shouldBe tt.value
    }
  }
}

private class TinyTypeTest private (val value: String) extends AnyVal with StringTinyType
private object TinyTypeTest extends TinyTypeFactory[TinyTypeTest](new TinyTypeTest(_)) {

  val invalidValue                   = "abc"
  val invalidValueMessage            = s"$typeName cannot have '$invalidValue' value"
  val invalidChar                    = "!"
  val invalidCharMessage             = s"$invalidChar is not allowed"
  val invalidForTransformation       = "10"
  val invalidTransformationException = new IllegalArgumentException("transformation error")

  override val transform: String => Either[Throwable, String] = {
    case `invalidForTransformation` => Left(invalidTransformationException)
    case other                      => Right(other.toString)
  }

  addConstraint(
    check = _ != invalidValue,
    message = _ => invalidValueMessage
  )

  addConstraint(
    check = !_.contains(invalidChar),
    message = _ => invalidCharMessage
  )
}
