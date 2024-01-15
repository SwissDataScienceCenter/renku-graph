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

package io.renku.knowledgegraph

import cats.effect.IO
import cats.syntax.all._
import io.renku.core.client.Generators.{resultFailures => coreResultFailures}
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.serverErrorHttpStatuses
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.http.RenkuEntityCodec
import munit.{CatsEffectSuite, ScalaCheckEffectSuite}
import org.scalacheck.effect.PropF.forAllF

class FailureSpec extends CatsEffectSuite with ScalaCheckEffectSuite with RenkuEntityCodec {

  test("toResponse should turn the failure into a Response with failure's status and message in the body") {
    forAllF(Generators.failures) { failure =>
      val response = failure.toResponse[IO]

      IO(response.status).map(assertEquals(_, failure.status)) >>
        response.as[Message].map(assertEquals(_, failure.message))
    }
  }

  test("Simple.detailedMessage should return the message") {

    val failure = Generators.simpleFailures.generateOne

    assertEquals(failure.detailedMessage, failure.message.show)
    assertEquals(failure.detailedMessage, failure.getMessage)
  }

  test("WithCause.detailedMessage should return the message") {

    val failure = Generators.withCauseFailures(causeGen = exceptions).generateOne

    assertEquals(failure.detailedMessage, failure.message.show)
    assertEquals(failure.detailedMessage, failure.getMessage)
  }

  test {
    "WithCause.detailedMessage should " +
      "return the message with the detailedMessage from the cause if it's a Core Result.Failure"
  } {

    val coreResultFailure = coreResultFailures.generateOne
    val failure           = Generators.withCauseFailures(causeGen = coreResultFailure).generateOne

    assertEquals(failure.detailedMessage, show"${failure.message}; ${coreResultFailure.detailedMessage}")
    assertEquals(failure.getMessage, failure.message.show)
  }

  test {
    "WithCause.detailedMessage should " +
      "return a message where no repetition of the exception message occurs"
  } {

    val coreResultFailure = coreResultFailures.generateOne
    val failure = Failure(serverErrorHttpStatuses.generateOne,
                          Message.Error.fromExceptionMessage(coreResultFailure),
                          coreResultFailure
    )

    assertEquals(failure.detailedMessage, coreResultFailure.detailedMessage)
    assertEquals(failure.getMessage, failure.message.show)
  }
}
