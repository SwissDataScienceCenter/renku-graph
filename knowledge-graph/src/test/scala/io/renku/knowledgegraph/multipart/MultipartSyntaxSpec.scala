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

package io.renku.knowledgegraph.multipart

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{ints, nonEmptyStrings, positiveInts}
import io.renku.knowledgegraph.multipart.syntax._
import io.renku.tinytypes.constraints.PositiveInt
import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}
import org.http4s.MalformedMessageBodyFailure
import org.http4s.multipart.Part
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class MultipartSyntaxSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with EitherValues {

  "intTinyTypeEntityDecoder" should {

    "decode IntTypeType value" in {

      val tt = positiveInts().map(_.value).generateAs(IntTT)

      tt.asPart[IO](nonEmptyStrings().generateOne).as[IntTT].asserting(_ shouldBe tt)
    }

    "fail if part value is not an int" in {

      val value = nonEmptyStrings().generateOne

      Part
        .formData[IO](nonEmptyStrings().generateOne, value)
        .as[IntTT]
        .assertThrowsWithMessage[MalformedMessageBodyFailure](
          s"Malformed message body: '$value' is not valid ${IntTT.typeName}"
        )
    }

    "fail if part value is not a valid TT int" in {

      val value = ints(max = -1).generateOne

      val message = IntTT.from(value).left.value.getMessage

      Part
        .formData[IO](nonEmptyStrings().generateOne, value.toString)
        .as[IntTT]
        .assertThrowsWithMessage[MalformedMessageBodyFailure](s"Malformed message body: $message")
    }
  }
}

private class IntTT private (val value: Int) extends AnyVal with IntTinyType
private object IntTT extends TinyTypeFactory[IntTT](new IntTT(_)) with PositiveInt[IntTT] {
  implicit val factory: TinyTypeFactory[IntTT] = this
}
