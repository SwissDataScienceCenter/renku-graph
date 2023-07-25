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

package io.renku.http.server

import cats.effect.IO
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.CustomAsyncIOSpec
import org.http4s.ParseFailure
import org.http4s.Status._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class QueryParameterToolsSpec extends AsyncWordSpec with CustomAsyncIOSpec with should.Matchers {

  import QueryParameterTools._

  "toBadRequest" should {

    "return a BAD_REQUEST response containing JSON body with information about the query parameter name and validation errors" in {

      val parseFailuresList  = parseFailures.generateNonEmptyList()
      val badRequestResponse = toBadRequest[IO]

      for {
        response <- badRequestResponse(parseFailuresList)
        _ = response.status shouldBe BadRequest
        r <- response.as[Message].asserting {
               _ shouldBe Message.Error.unsafeApply(parseFailuresList.toList.map(_.message).mkString("; "))
             }
      } yield r
    }
  }

  private lazy val parseFailures: Gen[ParseFailure] = for {
    sanitized <- nonEmptyStrings()
    details   <- nonEmptyStrings()
  } yield ParseFailure(sanitized, details)
}
