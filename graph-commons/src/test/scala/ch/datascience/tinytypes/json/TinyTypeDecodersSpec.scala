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

package ch.datascience.tinytypes.json

import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

import DecodingTestTypes._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import eu.timepit.refined.api.Refined
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class TinyTypeDecodersSpec extends WordSpec {

  import TinyTypeDecoders._

  "blankToNone" should {

    "map a non-blank String value to a NonBlank" in {
      val value = nonEmptyStrings().generateOne
      blankToNone(Some(value)) shouldBe Some(Refined.unsafeApply(value))
    }

    "map a blank String value to None" in {
      val value = blankStrings().generateOne
      blankToNone(Some(value)) shouldBe None
    }

    "map None to None" in {
      blankToNone(None) shouldBe None
    }
  }

  "toOption" should {

    "map a valid for the type non-blank value to an instance of that type" in {
      val value: NonBlank = Refined.unsafeApply(nonEmptyStrings().generateOne)
      toOption[StringTestType](StringTestType)(Option(value)) shouldBe Right(Some(StringTestType(value.toString())))
    }

    "map a non-valid for the type non-blank value to an error" in {
      val value: NonBlank = Refined.unsafeApply(StringTestType.InvalidValue)
      toOption[StringTestType](StringTestType)(Option(value)) shouldBe a[Left[_, _]]
    }

    "map None to None" in {
      toOption[StringTestType](StringTestType)(None) shouldBe Right(None)
    }
  }

  "stringDecoder" should {

    "decode JSON String value" in {
      val value = nonEmptyStrings().generateOne
      json"""$value""".as[StringTestType] shouldBe Right(StringTestType(value))
    }
  }

  "localDateDecoder" should {

    "decode JSON String value" in {
      val value = localDates.generateOne
      json"""$value""".as[LocalDateTestType] shouldBe Right(LocalDateTestType(value))
    }
  }

  "instantDecoder" should {

    "decode JSON String value in the Zulu format" in {
      val value = timestamps.generateOne
      json"""$value""".as[InstantTestType] shouldBe Right(InstantTestType(value))
    }

    "decode JSON String value with the offset" in {
      val value = zonedDateTimes.generateOne
      json"""${ISO_OFFSET_DATE_TIME format value}""".as[InstantTestType] shouldBe Right(
        InstantTestType(value.toOffsetDateTime.atZoneSameInstant(UTC).toInstant)
      )
    }
  }
}
