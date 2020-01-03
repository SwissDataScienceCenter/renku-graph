/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import cats.effect.Clock
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Warn
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import com.typesafe.config.ConfigFactory
import io.prometheus.client.Histogram
import org.scalacheck.Gen
import org.scalacheck.Gen.finiteDuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.{Success, Try}

class ExecutionTimeRecorderSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "measureExecutionTime" should {

    "measure execution time of the given block and provide it to the output" in new TestCase {

      val startTime = Gen.choose(0L, 10000000L).generateOne
      (clock
        .monotonic(_: TimeUnit))
        .expects(MILLISECONDS)
        .returning(context.pure(startTime))

      val elapsedTime = elapsedTimes.generateOne
      (clock
        .monotonic(_: TimeUnit))
        .expects(MILLISECONDS)
        .returning(context.pure(startTime + elapsedTime.value))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(context.pure(blockOut))

      executionTimeRecorder.measureExecutionTime[String] {
        block()
      } shouldBe context.pure(elapsedTime -> blockOut)
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

    "made the given histogram to collect process' execution time" in new TestCase {
      val histogram                      = Histogram.build("metric", "help").create()
      override val executionTimeRecorder = new ExecutionTimeRecorder(loggingThreshold, logger, Some(histogram))

      (clock
        .monotonic(_: TimeUnit))
        .expects(MILLISECONDS)
        .returning(context.pure(positiveLongs().generateOne.value))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(context.pure(blockOut))

      val blockExecutionTime = positiveInts(max = 100).generateOne.value
      executionTimeRecorder.measureExecutionTime[String] {
        Thread sleep blockExecutionTime
        block()
      }

      val Some(sample) = histogram.collect().asScala.flatMap(_.samples.asScala).lastOption.map(_.value)
      sample should be >= blockExecutionTime.toDouble / 1000
    }
  }

  "logExecutionTimeWhen" should {

    "log warning with the phrase returned from the given partial function if it gets applied " +
      "and the elapsed time is >= threshold" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.retryUntil(_.value >= loggingThreshold.value).generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      context.pure(elapsedTime -> blockOut) map logExecutionTimeWhen {
        case _ => blockExecutionMessage
      } shouldBe context.pure(blockOut)

      logger.loggedOnly(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
    }

    "not log a message if the given partial function does get applied " +
      "but the elapsed time is < threshold" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = ElapsedTime(loggingThreshold.value - 1)
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      context.pure(elapsedTime -> blockOut) map logExecutionTimeWhen {
        case _ => blockExecutionMessage
      } shouldBe context.pure(blockOut)

      logger.expectNoLogs()
    }

    "not log a message if the given partial function does not get applied" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      context.pure(elapsedTime -> blockOut) map logExecutionTimeWhen {
        case "" => blockExecutionMessage
      } shouldBe context.pure(blockOut)

      logger.expectNoLogs()
    }
  }

  "logExecutionTime" should {

    "log warning with the given phrase when elapsed time is >= threshold" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.retryUntil(_.value >= loggingThreshold.value).generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      context
        .pure(elapsedTime -> blockOut)
        .map(logExecutionTime(blockExecutionMessage)) shouldBe context.pure(blockOut)

      logger.loggedOnly(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
    }

    "not log a message if the elapsed time is < threshold" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = ElapsedTime(loggingThreshold.value - 1)
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      context
        .pure(elapsedTime -> blockOut)
        .map(logExecutionTime(blockExecutionMessage)) shouldBe context.pure(blockOut)

      logger.expectNoLogs()
    }
  }

  "apply" should {

    "read the logging threshold from 'logging.elapsed-time-threshold' and instantiate the recorder with it" in {
      implicit val clock: Clock[Try] = mock[Clock[Try]]
      val context = MonadError[Try, Throwable]

      forAll(finiteDuration retryUntil (_.toMillis > 0)) { threshold =>
        val config = ConfigFactory.parseMap(
          Map(
            "logging" -> Map(
              "elapsed-time-threshold" -> threshold.toString()
            ).asJava
          ).asJava
        )

        val logger                         = TestLogger[Try]()
        val Success(executionTimeRecorder) = ExecutionTimeRecorder[Try](logger, config)

        val elapsedTime           = ElapsedTime(threshold.toMillis)
        val blockOut              = nonEmptyStrings().generateOne
        val blockExecutionMessage = "block executed"

        context.pure(elapsedTime -> blockOut) map executionTimeRecorder.logExecutionTimeWhen {
          case _ => blockExecutionMessage
        } shouldBe context.pure(blockOut)

        logger.loggedOnly(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
      }
    }
  }

  private val context = MonadError[Try, Throwable]

  private trait TestCase {

    val block = mockFunction[Try[String]]

    val loggingThreshold = ElapsedTime(1000)
    val logger           = TestLogger[Try]()
    implicit val clock: Clock[Try] = mock[Clock[Try]]
    val executionTimeRecorder = new ExecutionTimeRecorder(loggingThreshold, logger, maybeHistogram = None)
  }
}
