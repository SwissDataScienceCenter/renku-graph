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

package io.renku.triplesgenerator.events.categories.tsprovisioning

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.triplesgenerator.events.categories.{ProcessingNonRecoverableError, ProcessingRecoverableError}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError.{LogWorthyRecoverableError, SilentRecoverableError}
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class RecoverableErrorsRecoverySpec extends AnyWordSpec with should.Matchers with TableDrivenPropertyChecks {
  import RecoverableErrorsRecovery._

  "maybeRecoverableError" should {

    forAll(
      Table(
        ("Problem name", "Exception", "Expected Failure type"),
        ("connection problem",
         ConnectivityException(nonEmptyStrings().generateOne, exceptions.generateOne),
         shouldBeLogWorthy
        ),
        ("client problem", ClientException(nonEmptyStrings().generateOne, exceptions.generateOne), shouldBeLogWorthy),
        ("BadGateway",
         UnexpectedResponseException(Status.BadGateway, nonEmptyStrings().generateOne),
         shouldBeLogWorthy
        ),
        ("ServiceUnavailable",
         UnexpectedResponseException(Status.ServiceUnavailable, nonEmptyStrings().generateOne),
         shouldBeLogWorthy
        ),
        ("Forbidden", UnexpectedResponseException(Status.Forbidden, nonEmptyStrings().generateOne), shouldBeSilent),
        ("Unauthorized",
         UnexpectedResponseException(Status.Unauthorized, nonEmptyStrings().generateOne),
         shouldBeSilent
        )
      )
    ) { case (problemName, exception, failureTypeAssertion) =>
      s"return a Recoverable Failure for $problemName" in {
        val Success(Left(failure: ProcessingRecoverableError)) =
          exception.raiseError[Try, Unit] recoverWith maybeRecoverableError[Try, Unit]

        failureTypeAssertion(failure)
      }
    }

    s"fail with MalformedRepository for ${Status.InternalServerError}" in {
      val exception = UnexpectedResponseException(Status.InternalServerError, nonEmptyStrings().generateOne)

      val Failure(failure) = exception.raiseError[Try, Unit] recoverWith maybeRecoverableError[Try, Unit]

      failure            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      failure.getMessage shouldBe exception.message
      failure.getCause   shouldBe exception.getCause
    }
  }

  private lazy val shouldBeLogWorthy = (failure: ProcessingRecoverableError) =>
    failure shouldBe a[LogWorthyRecoverableError]
  private lazy val shouldBeSilent = (failure: ProcessingRecoverableError) => failure shouldBe a[SilentRecoverableError]
}
