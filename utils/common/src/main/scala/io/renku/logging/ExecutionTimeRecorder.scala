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

import cats.syntax.all._
import cats.{Applicative, Monad}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.tinytypes.{LongTinyType, TinyTypeFactory}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

abstract class ExecutionTimeRecorder[F[_]](threshold: ElapsedTime) {

  def measureExecutionTime[A](
      block:               F[A],
      maybeHistogramLabel: Option[String Refined NonEmpty] = None
  ): F[(ElapsedTime, A)]

  def measureAndLogTime[A](message: PartialFunction[A, String])(
      block: F[A]
  )(implicit F: Monad[F], L: Logger[F]): F[A] =
    measureExecutionTime(block).flatMap(logExecutionTimeWhen(message))

  def logExecutionTimeWhen[A](
      message: PartialFunction[A, String]
  )(implicit F: Applicative[F], L: Logger[F]): ((ElapsedTime, A)) => F[A] = { resultAndTime =>
    logWarningIfAboveThreshold(resultAndTime, message.lift).as(resultAndTime._2)
  }

  def logExecutionTime[A](
      withMessage: => String
  )(implicit F: Applicative[F], L: Logger[F]): ((ElapsedTime, A)) => F[A] = { resultAndTime =>
    logWarningIfAboveThreshold(resultAndTime, Function.const(Some(withMessage))).as(resultAndTime._2)
  }

  private def logWarningIfAboveThreshold[A](
      resultAndTime: (ElapsedTime, A),
      withMessage:   A => Option[String]
  )(implicit F: Applicative[F], L: Logger[F]): F[Unit] = {
    val (elapsedTime, result) = resultAndTime
    withMessage(result)
      .filter(_ => elapsedTime >= threshold)
      .map(message => L.warn(s"$message in ${elapsedTime}ms"))
      .getOrElse(F.unit)
  }
}

object ExecutionTimeRecorder {

  def apply[F[_]](implicit etr: ExecutionTimeRecorder[F]): ExecutionTimeRecorder[F] = etr

  class ElapsedTime private (val value: Long) extends AnyVal with LongTinyType
  object ElapsedTime extends TinyTypeFactory[ElapsedTime](new ElapsedTime(_)) {
    addConstraint(
      check = _ >= 0,
      message = (_: Long) => s"$typeName cannot be < 0"
    )

    def apply(elapsedTime: FiniteDuration): ElapsedTime = ElapsedTime(elapsedTime.toMillis)
  }
}
