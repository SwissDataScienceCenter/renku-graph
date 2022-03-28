package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnauthorizedException, UnexpectedResponseException}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError.{AuthRecoverableError, LogWorthyRecoverableError}
import org.http4s.Status.{BadGateway, Forbidden, GatewayTimeout, ServiceUnavailable, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class RecoverableErrorsRecoverySpec extends AnyWordSpec with IOSpec with should.Matchers {

  val message      = nonEmptyStrings().generateOne
  val cause        = exceptions.generateOne
  val causeMessage = nonEmptyStrings().generateOne

  Set(
    "ConnectivityException" -> ConnectivityException(causeMessage, cause),
    "ClientException"       -> ClientException(causeMessage, cause)
  ) foreach { case (problemName, exception) =>
    s"return a LogWorthyRecoverableError for $problemName when handling an exception" in {
      val Left(error) = RecoverableErrorsRecovery
        .maybeRecoverableError[IO, Any](message)
        .apply(exception)
        .unsafeRunSync()
      error            shouldBe a[LogWorthyRecoverableError]
      error.getMessage shouldBe s"$message: $causeMessage"
    }
  }
  Set(
    "Unauthorized" -> UnexpectedResponseException(Unauthorized, causeMessage),
    "Forbidden"    -> UnexpectedResponseException(Forbidden, causeMessage)
  ) foreach { case (problemName, exception) =>
    s"return a AuthRecoverableError for $problemName when handling an exception" in {
      val Left(error) =
        RecoverableErrorsRecovery.maybeRecoverableError[IO, Any](message).apply(exception).unsafeRunSync()
      error            shouldBe a[AuthRecoverableError]
      error.getMessage shouldBe s"$message: $causeMessage"
    }
  }

  Set(
    "BadGateway"         -> UnexpectedResponseException(BadGateway, causeMessage),
    "ServiceUnavailable" -> UnexpectedResponseException(ServiceUnavailable, causeMessage),
    "GatewayTimeout"     -> UnexpectedResponseException(GatewayTimeout, causeMessage)
  ) foreach { case (problemName, exception) =>
    s"Return a LogWorthyRecoverableError the interpretation when a $problemName occurs" in {

      val Left(error) = RecoverableErrorsRecovery
        .maybeRecoverableError[IO, Any](message)
        .apply(exception)
        .unsafeRunSync()
      error            shouldBe a[LogWorthyRecoverableError]
      error.getMessage shouldBe s"$message: $causeMessage"
    }
  }

  "return an AuthRecoverableError when an UnauthorizedException" in {

    val Left(error) = RecoverableErrorsRecovery
      .maybeRecoverableError[IO, Any](message)
      .apply(UnauthorizedException)
      .unsafeRunSync()
    error            shouldBe a[AuthRecoverableError]
    error.getMessage shouldBe s"$message: ${UnauthorizedException.getMessage}"
  }

}
