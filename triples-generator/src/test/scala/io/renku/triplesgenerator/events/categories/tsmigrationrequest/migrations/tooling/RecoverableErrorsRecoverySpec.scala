package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError.LogWorthyRecoverableError
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

private class RecoverableErrorsRecoverySpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {
  import RecoverableErrorsRecovery._

  "maybeRecoverableError" should {

    forAll(
      Table(
        "Problem name"       -> "Exception",
        "connection problem" -> ConnectivityException(nonEmptyStrings().generateOne, exceptions.generateOne),
        "client problem"     -> ClientException(nonEmptyStrings().generateOne, exceptions.generateOne),
        "BadGateway"         -> UnexpectedResponseException(Status.BadGateway, nonEmptyStrings().generateOne),
        "ServiceUnavailable" -> UnexpectedResponseException(Status.ServiceUnavailable, nonEmptyStrings().generateOne),
        "Forbidden"          -> UnexpectedResponseException(Status.Forbidden, nonEmptyStrings().generateOne),
        "Unauthorized"       -> UnexpectedResponseException(Status.Unauthorized, nonEmptyStrings().generateOne)
      )
    ) { case (problemName, exception) =>
      s"return a Recoverable Failure for $problemName" in {
        val Success(Left(failure: ProcessingRecoverableError)) =
          exception.raiseError[Try, Unit] recoverWith maybeRecoverableError[Try, Unit]

        failure         shouldBe a[LogWorthyRecoverableError]
        failure.message shouldBe exception.getMessage
        failure.cause   shouldBe exception.getCause
      }
    }
  }
}
