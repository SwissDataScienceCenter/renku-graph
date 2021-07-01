/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.tinytypes.constraints

import UrlEncoder.urlEncode
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.RelativePathTinyType
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PathSegmentSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "PathSegment" should {

    "be a RelativePath" in {
      PathSegment shouldBe a[RelativePath]
    }

    "be a RelativePathTinyType" in {
      PathSegment(nonEmptyStrings().generateOne) shouldBe a[RelativePathTinyType]
    }
  }

  "apply" should {

    "url encode the given value and successfully instantiate" in {
      forAll(nonEmptyStrings(), Gen.oneOf("\\/", " "), nonEmptyStrings()) { (part1, part2, part3) =>
        val raw = s"$part1$part2$part3"
        PathSegment(raw).value shouldBe urlEncode(raw)
      }
    }
  }
}
