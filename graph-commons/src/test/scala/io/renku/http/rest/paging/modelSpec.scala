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

package io.renku.http.rest.paging

import io.renku.http.rest.paging.model.{Page, PerPage, Total}
import io.renku.tinytypes.constraints.{NonNegativeInt, PositiveInt}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class modelSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "Page" should {

    "be a PositiveInt constrained" in {
      Page shouldBe a[PositiveInt]
    }

  }

  "Page.first" should {

    "be Page(1)" in {
      (Page.first == Page(1)) shouldBe true
      Page.first              shouldBe Page(1)
    }
  }

  "PerPage" should {
    "be a PositiveInt constrained" in {
      PerPage shouldBe a[PositiveInt]
    }
  }

  "PerPage.default" should {

    "be PerPage(20)" in {
      PerPage.default shouldBe PerPage(20)
    }
  }

  "Total" should {
    "be a NonNegative constrained" in {
      Total shouldBe a[NonNegativeInt]
    }
  }
}
