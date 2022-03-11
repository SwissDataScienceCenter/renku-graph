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

package io.renku.http.server.version

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.httpStatuses
import io.renku.generators.Generators.Implicits._
import io.renku.http.server.EndpointTester._
import io.renku.testtools.IOSpec
import org.http4s.Method.GET
import org.http4s.implicits._
import org.http4s.{Request, Response}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RoutesSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "VersionEndpoint" should {

    "define routes for /GET version returning name and version in json" in new TestCase {
      val response = Response[IO](httpStatuses.generateOne)
      (() => endpoint.`GET /version`).expects().returning(response.pure[IO])

      routes.run(Request(GET, uri"/version")).unsafeRunSync() shouldBe response
    }
  }

  private trait TestCase {
    val endpoint          = mock[Endpoint[IO]]
    private val routesObj = new RoutesImpl[IO](endpoint)
    val routes            = routesObj() or notAvailableResponse
  }
}
