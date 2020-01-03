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

package ch.datascience.tinytypes.constraints

import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GitShaSpec extends WordSpec with ScalaCheckPropertyChecks {

  "GitSha" should {

    "be instantiatable for a valid sha" in {
      forAll(shas) { sha =>
        SomeGitSha(sha).toString shouldBe sha
      }
    }

    "throw an IllegalArgumentException for non-sha values" in {
      intercept[IllegalArgumentException] {
        SomeGitSha("abc")
      }.getMessage shouldBe "'abc' is not a valid Git sha"
    }

    "throw an IllegalArgumentException for a blank value" in {
      intercept[IllegalArgumentException] {
        SomeGitSha("   ")
      }.getMessage shouldBe "'   ' is not a valid Git sha"
    }
  }
}

private class SomeGitSha private (val value: String) extends AnyVal with StringTinyType

private object SomeGitSha extends TinyTypeFactory[SomeGitSha](new SomeGitSha(_)) with GitSha
