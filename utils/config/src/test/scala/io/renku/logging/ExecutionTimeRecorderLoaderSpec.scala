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

package io.renku.logging

import cats.effect.IO
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.testtools.CustomAsyncIOSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.jdk.CollectionConverters._

class ExecutionTimeRecorderLoaderSpec extends AsyncWordSpec with CustomAsyncIOSpec with should.Matchers {

  "apply" should {

    "read the logging threshold from 'logging.elapsed-time-threshold' and instantiate the recorder with it" in {

      val threshold = Gen.finiteDuration.retryUntil(_.toMillis > 0).generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "logging" -> Map(
            "elapsed-time-threshold" -> threshold.toString
          ).asJava
        ).asJava
      )

      implicit val logger: TestLogger[IO] = TestLogger[IO]()
      val executionTimeRecorder = ExecutionTimeRecorderLoader[IO](config).unsafeRunSync()

      val elapsedTime           = ElapsedTime(threshold.toMillis)
      val blockOut              = Generators.nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[IO]
        .flatMap(executionTimeRecorder.logExecutionTimeWhen { case _ => blockExecutionMessage })
        .asserting(_ shouldBe blockOut) >>
        logger.loggedOnlyF(Level.Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
    }
  }
}
