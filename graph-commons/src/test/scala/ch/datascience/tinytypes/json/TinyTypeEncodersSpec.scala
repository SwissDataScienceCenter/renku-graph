/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import java.time.format.DateTimeFormatter.{ISO_DATE, ISO_INSTANT}

import DecodingTestTypes._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import io.circe.Json
import io.circe.syntax._
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TinyTypeEncodersSpec extends AnyWordSpec with should.Matchers {

  import TinyTypeEncoders._

  "stringEncoder" should {

    "encode StringTinyType to Json" in {
      val value = nonEmptyStrings().generateOne
      StringTestType(value).asJson shouldBe Json.fromString(value)
    }
  }

  "relativePathEncoder" should {

    "encode RelativePathTinyType to Json" in {
      val value = nonEmptyStrings().generateOne
      RelativePathTestType(value).asJson shouldBe Json.fromString(value)
    }
  }

  "urlEncoder" should {

    "encode UrlTinyType to Json" in {
      val value = nonEmptyStrings().generateOne
      UrlTestType(value).asJson shouldBe Json.fromString(value)
    }
  }

  "intEncoder" should {

    "encode IntTinyType to Json" in {
      val value = Arbitrary.arbInt.arbitrary.generateOne
      IntTestType(value).asJson shouldBe Json.fromInt(value)
    }
  }

  "localDateEncoder" should {

    "encode LocalDateTinyType to Json" in {
      val value = localDates.generateOne
      LocalDateTestType(value).asJson shouldBe Json.fromString(ISO_DATE.format(value))
    }
  }

  "instantEncoder" should {

    "encode InstantTinyType to Json" in {
      val value = timestamps.generateOne
      InstantTestType(value).asJson shouldBe Json.fromString(ISO_INSTANT.format(value))
    }
  }
}
