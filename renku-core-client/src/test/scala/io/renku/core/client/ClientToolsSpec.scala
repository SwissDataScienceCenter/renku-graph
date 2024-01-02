/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.core.client

import Generators.resultDetailedFailures
import cats.effect.IO
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.Response
import org.http4s.Status.Ok
import org.http4s.circe._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class ClientToolsSpec extends AsyncWordSpec with CustomAsyncIOSpec with should.Matchers {

  "toResult" should {

    "decode the object from the 'result' into Result.success" in {

      val result   = nonEmptyStrings().generateOne
      val response = Response[IO](Ok).withEntity(json"""{"result": $result}""")

      ClientTools[IO].toResult[String](response).asserting(_ shouldBe Result.success(result))
    }

    "decode the failure into Result.Failure.Detailed" in {

      val failure = resultDetailedFailures.suchThat(_.maybeDevMessage.isEmpty).generateOne

      val response = Response[IO](Ok)
        .withEntity {
          json"""{
            "error": {
              "code":        ${failure.code},
              "userMessage": ${failure.userMessage}
            }
          }"""
        }

      ClientTools[IO].toResult[String](response).asserting(_ shouldBe failure)
    }

    "decode the failure into Result.Failure.Detailed in the presence of devMessage" in {

      val failure = resultDetailedFailures.suchThat(_.maybeDevMessage.isDefined).generateOne

      val response = Response[IO](Ok)
        .withEntity {
          json"""{
            "error": {
              "code":        ${failure.code},
              "userMessage": ${failure.userMessage},
              "devMessage":  ${failure.maybeDevMessage}
            }
          }"""
        }

      ClientTools[IO].toResult[String](response).asserting(_ shouldBe failure)
    }
  }
}
