/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.data.Message
import io.renku.data.Message._
import io.renku.generators.Generators.countingGen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class FailureSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with ScalaCheckPropertyChecks {

  "toResponse" should {

    forAll(Generators.failures, countingGen) { (failure, cnt) =>
      s"turn the failure into a Response with failure's status and message in the body #$cnt" in {

        val response = failure.toResponse[IO]

        IO(response.status).asserting(_ shouldBe failure.status) >>
          response.as[Message].asserting(_ shouldBe failure.message)
      }
    }
  }
}
