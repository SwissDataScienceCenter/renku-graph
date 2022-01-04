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

package io.renku.http.server

import EndpointTester._
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.testtools.IOSpec
import org.http4s.ParseFailure
import org.http4s.Status._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class QueryParameterToolsSpec extends AnyWordSpec with IOSpec with should.Matchers {

  import QueryParameterTools._

  "toBadRequest" should {

    "return a BAD_REQUEST response containing JSON body with information about the query parameter name and validation errors" in {
      val parseFailuresList  = parseFailures.generateNonEmptyList()
      val badRequestResponse = toBadRequest[IO]

      val response = badRequestResponse(parseFailuresList).unsafeRunSync()

      response.status shouldBe BadRequest
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
        parseFailuresList.toList.map(_.message).mkString("; ")
      )
    }
  }

  private lazy val parseFailures: Gen[ParseFailure] = for {
    sanitized <- nonEmptyStrings()
    details   <- nonEmptyStrings()
  } yield ParseFailure(sanitized, details)
}
