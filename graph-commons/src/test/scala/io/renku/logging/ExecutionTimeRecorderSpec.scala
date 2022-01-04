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

package io.renku.logging

import cats.effect.{IO, Temporal}
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.Histogram
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.testtools.IOSpec
import org.scalacheck.Gen.finiteDuration
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Try

class ExecutionTimeRecorderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "measureExecutionTime" should {

    "measure execution time of the given block and provide it to the output" in new TestCase {

      val elapsedTime = durations(min = 100 millis, max = 500 millis).map(ElapsedTime(_)).generateOne
      val blockOut    = nonEmptyStrings().generateOne
      block.expects().returning(Temporal[IO].delayBy(blockOut.pure[IO], elapsedTime.value millis))

      val actualElapsedTime -> actualOut = executionTimeRecorder.measureExecutionTime[String](block()).unsafeRunSync()

      actualElapsedTime should be > elapsedTime
      actualOut       shouldBe blockOut
    }

    "let the block failure propagate" in new TestCase {

      val exception = exceptions.generateOne
      block.expects().returning(exception.raiseError[IO, String])

      intercept[Exception] {
        executionTimeRecorder
          .measureExecutionTime[String](block())
          .unsafeRunSync()
      } shouldBe exception
    }

    "made the given histogram to collect process' execution time - case without a label" in new TestCase {
      val histogram                      = Histogram.build("metric", "help").create()
      override val executionTimeRecorder = new ExecutionTimeRecorderImpl(loggingThreshold, Some(histogram))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(blockOut.pure[IO])

      val blockExecutionTime = positiveInts(max = 100).generateOne.value
      executionTimeRecorder
        .measureExecutionTime[String] {
          Temporal[IO].delayBy(block(), blockExecutionTime millis)
        }
        .unsafeRunSync()

      val Some(sample) = histogram.collect().asScala.flatMap(_.samples.asScala).lastOption
      sample.value                should be >= blockExecutionTime.toDouble / 1000
      sample.labelNames.asScala shouldBe empty
    }

    "made the given histogram to collect process' execution time - case with a label" in new TestCase {
      val label: String Refined NonEmpty = "label"
      val histogram                      = Histogram.build("metric", "help").labelNames(label.value).create()
      override val executionTimeRecorder = new ExecutionTimeRecorderImpl(loggingThreshold, Some(histogram))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(blockOut.pure[IO])

      val blockExecutionTime = positiveInts(max = 100).generateOne.value
      executionTimeRecorder
        .measureExecutionTime[String](
          Temporal[IO].delayBy(block(), blockExecutionTime millis),
          Some(label)
        )
        .unsafeRunSync()

      val Some(sample) = histogram.collect().asScala.flatMap(_.samples.asScala).lastOption
      sample.value              should be >= blockExecutionTime.toDouble / 1000
      sample.labelNames.asScala should contain only label.value
    }

    "log an error when collecting process' execution time fails due to histogram misconfiguration" in new TestCase {
      val label: String Refined NonEmpty = "label"
      val histogramName                  = "metric"
      val histogram                      = Histogram.build(histogramName, "help").labelNames(label.value).create()
      override val executionTimeRecorder = new ExecutionTimeRecorderImpl(loggingThreshold, Some(histogram))

      val blockOut = nonEmptyStrings().generateOne
      block.expects().returning(blockOut.pure[IO])

      val blockExecutionTime = positiveInts(max = 100).generateOne.value
      executionTimeRecorder
        .measureExecutionTime[String] {
          Temporal[IO].delayBy(block(), blockExecutionTime millis)
        }
        .unsafeRunSync()

      histogram.collect().asScala.flatMap(_.samples.asScala).lastOption shouldBe None

      logger.loggedOnly(Error(s"$histogramName histogram labels not configured correctly"))
    }
  }

  "logExecutionTimeWhen" should {

    "log warning with the phrase returned from the given partial function if it gets applied " +
      "and the elapsed time is >= threshold" in new TestCase {
        import executionTimeRecorder._

        val elapsedTime           = elapsedTimes.retryUntil(_.value >= loggingThreshold.value).generateOne
        val blockOut              = nonEmptyStrings().generateOne
        val blockExecutionMessage = "block executed"

        (elapsedTime -> blockOut).pure[Try] map logExecutionTimeWhen { case _ =>
          blockExecutionMessage
        } shouldBe blockOut.pure[Try]

        logger.loggedOnly(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
      }

    "not log a message if the given partial function does get applied " +
      "but the elapsed time is < threshold" in new TestCase {
        import executionTimeRecorder._

        val elapsedTime           = ElapsedTime(loggingThreshold.value - 1)
        val blockOut              = nonEmptyStrings().generateOne
        val blockExecutionMessage = "block executed"

        (elapsedTime -> blockOut).pure[Try] map logExecutionTimeWhen { case _ =>
          blockExecutionMessage
        } shouldBe blockOut.pure[Try]

        logger.expectNoLogs()
      }

    "not log a message if the given partial function does not get applied" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut).pure[Try] map logExecutionTimeWhen { case "" =>
        blockExecutionMessage
      } shouldBe blockOut.pure[Try]

      logger.expectNoLogs()
    }
  }

  "logExecutionTime" should {

    "log warning with the given phrase when elapsed time is >= threshold" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = elapsedTimes.retryUntil(_.value >= loggingThreshold.value).generateOne
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[Try]
        .map(logExecutionTime(blockExecutionMessage)) shouldBe blockOut.pure[Try]

      logger.loggedOnly(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
    }

    "not log a message if the elapsed time is < threshold" in new TestCase {
      import executionTimeRecorder._

      val elapsedTime           = ElapsedTime(loggingThreshold.value - 1)
      val blockOut              = nonEmptyStrings().generateOne
      val blockExecutionMessage = "block executed"

      (elapsedTime -> blockOut)
        .pure[Try]
        .map(logExecutionTime(blockExecutionMessage)) shouldBe blockOut.pure[Try]

      logger.expectNoLogs()
    }
  }

  "apply" should {

    "read the logging threshold from 'logging.elapsed-time-threshold' and instantiate the recorder with it" in {
      forAll(finiteDuration retryUntil (_.toMillis > 0)) { threshold =>
        val config = ConfigFactory.parseMap(
          Map(
            "logging" -> Map(
              "elapsed-time-threshold" -> threshold.toString()
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
          .map(executionTimeRecorder.logExecutionTimeWhen { case _ => blockExecutionMessage })
          .unsafeRunSync() shouldBe blockOut

        logger.loggedOnly(Warn(s"$blockExecutionMessage in ${elapsedTime}ms"))
      }
    }
  }

  private trait TestCase {

    val block = mockFunction[IO[String]]

    val loggingThreshold = ElapsedTime(1000 millis)
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executionTimeRecorder = new ExecutionTimeRecorderImpl[IO](loggingThreshold, maybeHistogram = None)
  }
}
