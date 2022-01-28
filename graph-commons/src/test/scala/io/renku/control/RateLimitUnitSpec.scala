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

package io.renku.control

import io.renku.control.RateLimitUnit._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class RateLimitUnitSpec extends AnyWordSpec with should.Matchers {

  "multiplierFor" should {

    "return valid NANOS multiplier for all units" in {
      Second.multiplierFor(NANOSECONDS) shouldBe 1e9
      Minute.multiplierFor(NANOSECONDS) shouldBe 6e10
      Hour.multiplierFor(NANOSECONDS)   shouldBe 3.6e12
      Day.multiplierFor(NANOSECONDS)    shouldBe 8.64e13
    }

    "return valid MILLIS multiplier for all units" in {
      Second.multiplierFor(MILLISECONDS) shouldBe 1000d
      Minute.multiplierFor(MILLISECONDS) shouldBe 1000 * 60d
      Hour.multiplierFor(MILLISECONDS)   shouldBe 1000 * 60 * 60d
      Day.multiplierFor(MILLISECONDS)    shouldBe 1000 * 60 * 60 * 24d
    }
  }
}
