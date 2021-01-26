package ch.datascience.http.server.security

import cats.effect.IO
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.server.security.EndpointSecurityException.{AuthenticationFailure, AuthorizationFailure}
import org.http4s.MediaType._
import org.http4s.Status.{Forbidden, Unauthorized}
import org.http4s.headers.`Content-Type`
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSecurityExceptionSpec extends AnyWordSpec with should.Matchers {

  "AuthenticationFailure.toHttpResponse" should {

    s"return an $Unauthorized response with a relavant error message" in {
      val response = AuthenticationFailure.toHttpResponse[IO]

      response.status                           shouldBe Unauthorized
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("User authentication failure")
    }
  }

  "AuthorizationFailure.toHttpResponse" should {

    s"return an $Forbidden response with a relavant error message" in {
      val response = AuthorizationFailure.toHttpResponse[IO]

      response.status                           shouldBe Forbidden
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("User not authorized failure")
    }
  }
}
