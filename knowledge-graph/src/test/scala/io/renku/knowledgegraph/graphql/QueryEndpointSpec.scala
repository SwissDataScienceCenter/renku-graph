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

package io.renku.knowledgegraph.graphql

import cats.effect.IO
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.LineageFinder
import io.renku.testtools.IOSpec
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.scalamock.matchers.MatcherBase
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import sangria.execution.{ExceptionHandler, QueryAnalysisError}

class QueryEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "handleQuery" should {

    "respond with OK and the results found by the Query Runner when there are no variables in the query" in new TestCase {

      val request = Request[IO](Method.POST, uri"graphql").withEntity(queryWithNoVariablesPayload)

      val queryResult = jsons.generateOne
      (queryRunner
        .run(_: UserQuery, _: Option[AuthUser]))
        .expects(queryMatchingRequest, maybeAuthUser)
        .returning(IO.pure(queryResult))

      val response = handleQuery(request, maybeAuthUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe queryResult
    }

    "respond with OK and the results found by the Query Runner when there are variables in the query" in new TestCase {

      val request = Request[IO](Method.POST, uri"graphql").withEntity(queryWithVariablesPayload)

      val queryResult = jsons.generateOne
      (queryRunner
        .run(_: UserQuery, _: Option[AuthUser]))
        .expects(queryAndVariablesMatchingRequest, maybeAuthUser)
        .returning(IO.pure(queryResult))

      val response = handleQuery(request, maybeAuthUser).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe queryResult
    }

    "respond with BAD_REQUEST if the given query is invalid" in new TestCase {

      val request = Request[IO](Method.POST, uri"graphql").withEntity("")

      val response = handleQuery(request, maybeAuthUser).unsafeRunSync()

      response.status                               shouldBe BadRequest
      response.contentType                          shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync().value should include("Invalid JSON")
    }

    "respond with BAD_REQUEST if running the query fails with QueryAnalysisError" in new TestCase {

      val request = Request[IO](Method.POST, uri"graphql").withEntity(queryWithNoVariablesPayload)

      val exception = queryAnalysisErrors.generateOne
      (queryRunner
        .run(_: UserQuery, _: Option[AuthUser]))
        .expects(queryMatchingRequest, maybeAuthUser)
        .returning(IO.raiseError(exception))

      val response = handleQuery(request, maybeAuthUser).unsafeRunSync()

      response.status                   shouldBe BadRequest
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(exception).asJson
    }

    "respond with INTERNAL_SERVER_ERROR if running the query fails" in new TestCase {

      val request = Request[IO](Method.POST, uri"graphql").withEntity(queryWithNoVariablesPayload)

      val exception = exceptions.generateOne

      (queryRunner
        .run(_: UserQuery, _: Option[AuthUser]))
        .expects(queryMatchingRequest, maybeAuthUser)
        .returning(IO.raiseError(exception))

      val response = handleQuery(request, maybeAuthUser).unsafeRunSync()

      response.status                   shouldBe InternalServerError
      response.contentType              shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(exception).asJson
    }
  }

  private trait TestCase {

    val maybeAuthUser = authUsers.generateOption
    val queryRunner   = mock[IOQueryRunner]
    val handleQuery   = new QueryEndpointImpl[IO](queryRunner).handleQuery _

    private val queryWithNoVariables = """{ resource { property } }"""
    lazy val queryWithNoVariablesPayload = json"""
      {                                                      
        "query": $queryWithNoVariables
      }"""
    val queryMatchingRequest: MatcherBase = argAssert { userQuery: UserQuery =>
      userQuery.query.source shouldBe Some(queryWithNoVariables)
      ()
    }

    private val queryWithVariables = """query($variable: Type!) { resource(variable: $variable) { property } }"""
    lazy val queryWithVariablesPayload = json"""
      {
        "query": $queryWithVariables,
        "variables": {"variable": "value"}
      }"""
    val queryAndVariablesMatchingRequest: MatcherBase = argAssert { userQuery: UserQuery =>
      userQuery.query.source shouldBe Some(queryWithVariables)
      userQuery.variables    shouldBe Map("variable" -> "value")
      ()
    }
  }

  private val queryAnalysisErrors =
    nonEmptyStrings()
      .map { message =>
        new Exception(message) with QueryAnalysisError {
          override def exceptionHandler = ExceptionHandler.empty
        }
      }

  private class IOLineageQueryContext(override val lineageFinder: LineageFinder[IO],
                                      override val maybeUser:     Option[AuthUser]
  ) extends LineageQueryContext[IO](lineageFinder, maybeUser)

  private trait IOQueryRunner extends QueryRunner[IO, LineageQueryContext[IO]]
}
