/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.server

import EndpointTester._
import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.http4s.ParseFailure
import org.http4s.Status._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class QueryParameterToolsSpec extends WordSpec {

  import QueryParameterTools._

  "toBadRequest" should {

    "return a BAD_REQUEST response containing JSON body with information about the query parameter name and validation errors" in {
      val parseFailuresList = nonEmptyList(parseFailures).generateOne

      val response = toBadRequest[IO]()(parseFailuresList).unsafeRunSync()

      response.status shouldBe BadRequest
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
        parseFailuresList.toList.map(_.message).mkString("; ")
      )
    }
  }

  private val parseFailures: Gen[ParseFailure] = for {
    sanitized <- nonEmptyStrings()
    details   <- nonEmptyStrings()
  } yield ParseFailure(sanitized, details)
}
