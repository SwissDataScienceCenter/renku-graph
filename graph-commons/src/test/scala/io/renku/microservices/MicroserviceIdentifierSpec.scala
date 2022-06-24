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

package io.renku.microservices

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDateTime

class MicroserviceIdentifierSpec extends AnyWordSpec with should.Matchers {

  "generate" should {

    "produce unique identifiers in the format yyyyMMddHHmmss-{random 4 digits int}" in {
      val now = LocalDateTime.now

      val id = MicroserviceIdentifier.generate(() => now)

      id     shouldBe a[MicroserviceIdentifier]
      id.value should fullyMatch regex s"${now.getYear}${addPadding(now.getMonthValue)}${addPadding(
          now.getDayOfMonth
        )}${addPadding(now.getHour)}${addPadding(now.getMinute)}${addPadding(now.getSecond)}\\-\\d{4}"
    }
  }

  private def addPadding(value: Int) = f"$value%02d"
}
