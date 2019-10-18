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

import cats.MonadError
import cats.effect.Clock
import cats.implicits._
import ch.datascience.config.ConfigLoader.find
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.tinytypes.{LongTinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration._
import scala.language.higherKinds

class ExecutionTimeRecorder[Interpretation[_]](
    threshold:    ElapsedTime,
    logger:       Logger[Interpretation],
)(implicit clock: Clock[Interpretation], ME: MonadError[Interpretation, Throwable]) {

  def measureExecutionTime[BlockOut](block: => Interpretation[BlockOut]): Interpretation[(ElapsedTime, BlockOut)] =
    for {
      startTime   <- clock monotonic MILLISECONDS
      result      <- block
      endTime     <- clock monotonic MILLISECONDS
      elapsedTime <- ME.fromEither(ElapsedTime.from(endTime - startTime))
    } yield (elapsedTime, result)

  def logExecutionTimeWhen[BlockOut](
      condition: PartialFunction[BlockOut, String]
  ): ((ElapsedTime, BlockOut)) => BlockOut = {
    case (elapsedTime, blockOut) =>
      logWarningIfAboveThreshold(elapsedTime, blockOut, condition)
      blockOut
  }

  def logExecutionTime[BlockOut](withMessage: => String): ((ElapsedTime, BlockOut)) => BlockOut = {
    case (elapsedTime, blockOut) =>
      logWarningIfAboveThreshold(elapsedTime, blockOut, forAnyOutReturn(withMessage))
      blockOut
  }

  private def forAnyOutReturn[BlockOut](message: String): PartialFunction[BlockOut, String] = {
    case _ => message
  }

  private def logWarningIfAboveThreshold[BlockOut](
      elapsedTime: ElapsedTime,
      blockOut:    BlockOut,
      condition:   PartialFunction[BlockOut, String]
  ): Unit =
    if (elapsedTime.value >= threshold.value)
      (condition lift blockOut) foreach (message => logger.warn(s"$message in ${elapsedTime}ms"))
}

object ExecutionTimeRecorder {

  def apply[Interpretation[_]](
      logger:       Logger[Interpretation],
      config:       Config = ConfigFactory.load()
  )(implicit clock: Clock[Interpretation],
    ME:             MonadError[Interpretation, Throwable]): Interpretation[ExecutionTimeRecorder[Interpretation]] =
    for {
      duration  <- find[Interpretation, FiniteDuration]("logging.elapsed-time-threshold", config)
      threshold <- ME.fromEither(ElapsedTime from duration.toMillis)
    } yield new ExecutionTimeRecorder(threshold, logger)

  class ElapsedTime private (val value: Long) extends AnyVal with LongTinyType
  object ElapsedTime extends TinyTypeFactory[ElapsedTime](new ElapsedTime(_)) {
    addConstraint(
      check   = _ >= 0,
      message = (_: Long) => s"$typeName cannot be < 0"
    )
  }
}
