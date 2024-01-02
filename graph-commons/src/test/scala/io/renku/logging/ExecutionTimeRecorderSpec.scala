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

import cats.effect.{IO, Temporal}
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Warn
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.Histogram
import io.renku.testtools.CustomAsyncIOSpec
import org.scalacheck.Gen.finiteDuration
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ExecutionTimeRecorderSpec
    extends AsyncWordSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with BeforeAndAfterEach {

  "measureExecutionTime" should {

    "measure execution time of the given block and provide it to the output" in {

      val elapsedTime = durations(min = 100 millis, max = 500 millis).map(ElapsedTime(_)).generateOne
      val blockOut    = nonEmptyStrings().generateOne
      block.expects().returning(Temporal[IO].delayBy(blockOut.pure[IO], elapsedTime.value millis))

      executionTimeRecorder.measureExecutionTime[String](block()).asserting { case actualElapsedTime -> actualOut =>
        actualElapsedTime should be >= elapsedTime
        actualOut       shouldBe blockOut
      }
    }

    "let the block failure propagate" in {

      val exception = exceptions.generateOne
      block.expects().returning(exception.raiseError[IO, String])

      executionTimeRecorder
        .measureExecutionTime[String](block())
        .assertThrowsError[Exception](_ shouldBe exception)
    }

    "observe the process execution time and update the histogram - case with no label" in {

      val histogram             = mock[Histogram[IO]]
      val executionTimeRecorder = new ExecutionTimeRecorderImpl(loggingThreshold, Some(histogram))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(blockOut.pure[IO])

      val blockExecutionTime = positiveInts(max = 100).generateOne.value
      (histogram
        .observe(_: Option[String], _: FiniteDuration))
        .expects(where((l: Option[String], amt: FiniteDuration) => l.isEmpty && amt.toMillis >= blockExecutionTime))
        .returning(().pure[IO])

      executionTimeRecorder
        .measureExecutionTime[String] {
          Temporal[IO].delayBy(block(), blockExecutionTime millis)
        }
        .assertNoException
    }

    "observe the process execution time and update the histogram - case with a label" in {

      val label: String Refined NonEmpty = "label"
      val histogram             = mock[Histogram[IO]]
      val executionTimeRecorder = new ExecutionTimeRecorderImpl(loggingThreshold, Some(histogram))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(blockOut.pure[IO])

      val blockExecutionTime = positiveInts(max = 100).generateOne.value
      (histogram
        .observe(_: Option[String], _: FiniteDuration))
        .expects(
          where((l: Option[String], amt: FiniteDuration) =>
            l == Option(label.value) && amt.toMillis >= blockExecutionTime
          )
        )
        .returning(().pure[IO])

      executionTimeRecorder
        .measureExecutionTime[String](
          Temporal[IO].delayBy(block(), blockExecutionTime millis),
          Some(label)
        )
        .assertNoException >>
        IO(logger.expectNoLogs())
    }
  }

  "logExecutionTimeWhen" should {

    "log warning with the phrase returned from the given partial function if it gets applied " +
      "and the elapsed time is >= threshold" in {
        import executionTimeRecorder._

        val elapsedTime           = elapsedTimes.retryUntil(_.value >= loggingThreshold.value).generateOne
        val blockOut              = nonEmptyStrings().generateOne
        val blockExecutionMessage = "block executed"

        (elapsedTime -> blockOut)
          .pure[IO]
          .flatMap(logExecutionTimeWhen { case _ =>
            blockExecutionMessage
          })
          .asserting(_ shouldBe blockOut) >>
          logger.loggedOnlyF(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
      }

    "not log a message if the given partial function does get applied " +
      "but the elapsed time is < threshold" in {
        import executionTimeRecorder._

        val elapsedTime           = ElapsedTime(loggingThreshold.value - 1)
        val blockOut              = nonEmptyStrings().generateOne
        val blockExecutionMessage = "block executed"

        (elapsedTime -> blockOut)
          .pure[IO]
          .flatMap(logExecutionTimeWhen { case _ =>
            blockExecutionMessage
          })
          .asserting(_ shouldBe blockOut) >>
          logger.expectNoLogsF()
      }

    "not log a message if the given partial function does not get applied" in {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[IO]
        .flatMap(logExecutionTimeWhen { case "" =>
          blockExecutionMessage
        })
        .asserting(_ shouldBe blockOut) >>
        logger.expectNoLogsF()
    }
  }

  "logExecutionTime" should {

    "log warning with the given phrase when elapsed time is >= threshold" in {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.retryUntil(_.value >= loggingThreshold.value).generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[IO]
        .flatMap(logExecutionTime(blockExecutionMessage))
        .asserting(_ shouldBe blockOut) >>
        logger.loggedOnlyF(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
    }

    "not log a message if the elapsed time is < threshold" in {
      import executionTimeRecorder._

      val elapsedTime           = ElapsedTime(loggingThreshold.value - 1)
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[IO]
        .flatMap(logExecutionTime(blockExecutionMessage))
        .asserting(_ shouldBe blockOut) >>
        logger.expectNoLogsF()
    }
  }

  "apply" should {

    "read the logging threshold from 'logging.elapsed-time-threshold' and instantiate the recorder with it" in {

      val threshold = finiteDuration.retryUntil(_.toMillis > 0).generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "logging" -> Map(
            "elapsed-time-threshold" -> threshold.toString
          ).asJava
        ).asJava
      )

      implicit val logger: TestLogger[IO] = TestLogger[IO]()
      val executionTimeRecorder = ExecutionTimeRecorder[IO](config).unsafeRunSync()

      val elapsedTime           = ElapsedTime(threshold.toMillis)
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[IO]
        .flatMap(executionTimeRecorder.logExecutionTimeWhen { case _ => blockExecutionMessage })
        .asserting(_ shouldBe blockOut) >>
        logger.loggedOnlyF(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
    }
  }

  private lazy val block = mockFunction[IO[String]]

  private lazy val loggingThreshold = ElapsedTime(1000 millis)
  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private lazy val executionTimeRecorder = new ExecutionTimeRecorderImpl[IO](loggingThreshold, maybeHistogram = None)

  protected override def beforeEach() = {
    super.beforeEach()
    logger.reset()
  }
}
