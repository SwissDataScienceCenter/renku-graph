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
