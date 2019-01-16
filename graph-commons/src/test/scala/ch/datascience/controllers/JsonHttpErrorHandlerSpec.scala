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

package ch.datascience.controllers

import ch.datascience.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks
import play.api.libs.json.Json
import play.api.mvc.Request
import play.api.test.FakeRequest
import play.api.test.Helpers._

import scala.concurrent.ExecutionContext.Implicits.global

class JsonHttpErrorHandlerSpec extends WordSpec with PropertyChecks {

  "onClientError" should {

    "return response with the given status code and body comprised of requestId, and the given status and message" in new TestCase {

      forAll( errorStatusCodes, fakeRequests, nonEmptyStrings() ) { ( statusCode, request, message ) =>

        val result = errorHandler.onClientError( request, statusCode, message )

        status( result ) shouldBe statusCode
        contentType( result ) shouldBe Some( JSON )
        contentAsJson( result ) shouldBe Json.obj(
          "requestId" -> request.id,
          "statusCode" -> statusCode,
          "message" -> message
        )
      }
    }
  }

  "onServerError" should {

    "return response with the given status code and body comprised of requestId, and the given status and message" in new TestCase {

      forAll( fakeRequests, exceptions ) { ( request, exception ) =>

        val result = errorHandler.onServerError( request, exception )

        status( result ) shouldBe INTERNAL_SERVER_ERROR
        contentType( result ) shouldBe Some( JSON )
        contentAsJson( result ) shouldBe Json.obj(
          "requestId" -> request.id,
          "statusCode" -> INTERNAL_SERVER_ERROR,
          "message" -> exception.getMessage
        )
      }
    }
  }

  private trait TestCase {
    val errorHandler = new JsonHttpErrorHandler()( global )
  }

  private val errorStatusCodes: Gen[Int] = Gen.oneOf( 400, 401, 404, 500 )
  private val fakeRequests: Gen[Request[_]] = Gen.uuid.map( _ => FakeRequest() )
}
