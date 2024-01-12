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

package io.renku.http.server.security

import EndpointSecurityException.{AuthenticationFailure, AuthorizationFailure}
import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.http.RenkuEntityCodec
import io.renku.testtools.IOSpec
import org.http4s.MediaType._
import org.http4s.Status.{NotFound, Unauthorized}
import org.http4s.headers.`Content-Type`
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSecurityExceptionSpec extends AnyWordSpec with IOSpec with should.Matchers with RenkuEntityCodec {

  "AuthenticationFailure.toHttpResponse" should {

    s"return an $Unauthorized response with a relevant error message" in {
      val response = AuthenticationFailure.toHttpResponse[IO]

      response.status                      shouldBe Unauthorized
      response.contentType                 shouldBe Some(`Content-Type`(application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Error("User authentication failure")
    }
  }

  "AuthorizationFailure.toHttpResponse" should {

    s"return a $NotFound response with a relevant error message" in {
      val response = AuthorizationFailure.toHttpResponse[IO]

      response.status                      shouldBe NotFound
      response.contentType                 shouldBe Some(`Content-Type`(application.json))
      response.as[Message].unsafeRunSync() shouldBe Message.Error("Resource not found")
    }
  }
}
