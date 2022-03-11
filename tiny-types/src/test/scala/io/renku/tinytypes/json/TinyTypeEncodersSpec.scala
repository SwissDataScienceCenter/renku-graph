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

package io.renku.tinytypes.json

import io.circe.Json
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.tinytypes.TestTinyTypes._
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.format.DateTimeFormatter.{ISO_DATE, ISO_INSTANT}

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
      val value = httpUrls().generateOne
      UrlTestType(value).asJson shouldBe Json.fromString(value)
    }
  }

  "intEncoder" should {

    "encode IntTinyType to Json" in {
      val value = Arbitrary.arbInt.arbitrary.generateOne
      IntTestType(value).asJson shouldBe Json.fromInt(value)
    }
  }

  "longEncoder" should {

    "encode LongTinyType to Json" in {
      val value = Arbitrary.arbLong.arbitrary.generateOne
      LongTestType(value).asJson shouldBe Json.fromLong(value)
    }
  }

  "floatEncoder" should {

    "encode FloatTinyType to Json" in {
      val value = Arbitrary.arbFloat.arbitrary.generateOne
      Some(FloatTestType(value).asJson) shouldBe Json.fromFloat(value)
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

  "durationEncoder" should {

    "encode DurationTinyType to Json" in {
      val value = notNegativeJavaDurations.generateOne
      DurationTestType(value).asJson shouldBe Json.fromString(value.toString)
    }
  }
}
