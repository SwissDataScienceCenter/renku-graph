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

package io.renku.http.tinytypes

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.tinytypes.{IntTinyType, StringTinyType}
import org.http4s.Uri.Path.{Segment, SegmentEncoder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TinyTypeURIEncoderSpec extends AnyWordSpec with should.Matchers {

  import TinyTypeURIEncoder._

  "a SegmentEncoder" should {

    "be provided for StringTinyTypes" in {
      case class TestStringTinyType(value: String) extends StringTinyType
      val tt = nonEmptyStrings().generateAs(TestStringTinyType)

      implicitly[SegmentEncoder[TestStringTinyType]].toSegment(tt) shouldBe Segment(tt.value)
    }

    "be provided for IntTinyTypes" in {
      case class TestIntTinyType(value: Int) extends IntTinyType
      val tt = ints().generateAs(TestIntTinyType)

      implicitly[SegmentEncoder[TestIntTinyType]].toSegment(tt) shouldBe Segment(tt.value.toString)
    }
  }
}
