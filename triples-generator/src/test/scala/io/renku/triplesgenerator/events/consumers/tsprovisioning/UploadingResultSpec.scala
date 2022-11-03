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

package io.renku.triplesgenerator.events.consumers.tsprovisioning

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class UploadingResultSpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {

  "merge" should {

    forAll(
      Table(
        ("a", "b", "result"),
        (TestUploaded, TestUploaded, TestUploaded),
        (TestUploaded, TestRecoverableError, TestRecoverableError),
        (TestUploaded, TestNonRecoverableError, TestUploaded),
        (TestRecoverableError, TestRecoverableError, TestRecoverableError),
        (TestRecoverableError, TestNonRecoverableError, TestRecoverableError),
        (TestNonRecoverableError, TestNonRecoverableError, TestNonRecoverableError)
      )
    ) { (a, b, result) =>
      s"$a merge $b => $result" in {
        a merge b shouldBe result
        b merge a shouldBe result
      }
    }
  }

  private sealed trait TestResult extends UploadingResult[TestResult]

  private case object TestUploaded extends UploadingResult.Uploaded[TestResult] {
    override val toString = "Uploaded"
  }
  private case object TestRecoverableError extends UploadingResult.RecoverableError[TestResult] {
    override val cause: Throwable = exceptions.generateOne
    override val toString = "RecoverableError"
  }
  private case object TestNonRecoverableError extends UploadingResult.NonRecoverableError[TestResult] {
    override val cause: Throwable = exceptions.generateOne
    override val toString = "NonRecoverableError"
  }
}
