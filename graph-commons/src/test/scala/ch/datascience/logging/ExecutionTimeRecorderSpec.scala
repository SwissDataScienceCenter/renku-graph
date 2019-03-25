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

package ch.datascience.logging

import cats.implicits._
import cats.MonadError
import cats.effect.Clock
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec
import org.scalatest.Matchers._

import scala.concurrent.duration._
import scala.util.Try
import ch.datascience.generators.Generators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import org.scalacheck.Gen

class ExecutionTimeRecorderSpec extends WordSpec with MockFactory {

  "measureExecutionTime" should {

    "measure execution time of the given block and provide it to the output" in new TestCase {

      val startTime = Gen.choose(0L, 10000000L).generateOne
      (clock
        .monotonic(_: TimeUnit))
        .expects(MILLISECONDS)
        .returning(context.pure(startTime))

      val processExecutionTime = Gen.choose(0L, 1000L).generateOne
      (clock
        .monotonic(_: TimeUnit))
        .expects(MILLISECONDS)
        .returning(context.pure(startTime + processExecutionTime))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(context.pure(blockOut))

      executionTimeRecorder.measureExecutionTime[String] {
        block()
      } shouldBe context.pure(ElapsedTime(processExecutionTime) -> blockOut)
    }

    "let the block failure propagate" in new TestCase {

      val startTime = Gen.choose(0L, 10000000L).generateOne
      (clock
        .monotonic(_: TimeUnit))
        .expects(MILLISECONDS)
        .returning(context.pure(startTime))

      val exception = exceptions.generateOne
      block.expects().returning(context.raiseError(exception))

      executionTimeRecorder.measureExecutionTime[String] {
        block()
      } shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val block = mockFunction[Try[String]]

    implicit val clock        = mock[Clock[Try]]
    val executionTimeRecorder = new ExecutionTimeRecorder
  }
}
