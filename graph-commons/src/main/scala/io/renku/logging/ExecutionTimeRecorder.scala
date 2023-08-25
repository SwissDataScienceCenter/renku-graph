/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.effect.{Clock, Sync}
import cats.syntax.all._
import cats.{Applicative, Monad}
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.config.ConfigLoader.find
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.{Histogram, LabeledHistogram, SingleValueHistogram}
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

class ExecutionTimeRecorderImpl[F[_]: Sync: Clock: Logger](
    threshold:      ElapsedTime,
    maybeHistogram: Option[Histogram[F]]
) extends ExecutionTimeRecorder[F](threshold) {

  override def measureExecutionTime[A](
      block:               F[A],
      maybeHistogramLabel: Option[String Refined NonEmpty] = None
  ): F[(ElapsedTime, A)] = Clock[F]
    .timed {
      maybeHistogram match {
        case None => block
        case Some(histogram) =>
          for {
            maybeTimer <- maybeStartTimer(histogram, maybeHistogramLabel)
            result     <- block
            _          <- maybeTimer.map(_.observeDuration.void).getOrElse(().pure[F])
          } yield result
      }
    }
    .map { case (elapsedTime, result) => ElapsedTime(elapsedTime) -> result }

  private def maybeStartTimer(histogram: Histogram[F], maybeHistogramLabel: Option[String Refined NonEmpty]) =
    histogram -> maybeHistogramLabel match {
      case (h: SingleValueHistogram[F], None)    => h.startTimer().map(_.some)
      case (h: LabeledHistogram[F], Some(label)) => h.startTimer(label.value).map(_.some)
      case (h: SingleValueHistogram[F], Some(label)) =>
        Logger[F].error(s"Label $label sent for a Single Value Histogram ${h.name}") >> None.pure[F]
      case (h: LabeledHistogram[F], None) =>
        Logger[F].error(s"No label sent for a Labeled Histogram ${h.name}") >> None.pure[F]
    }
}

object ExecutionTimeRecorder {

  def apply[F[_]](implicit etr: ExecutionTimeRecorder[F]): ExecutionTimeRecorder[F] = etr

  def apply[F[_]: Sync: Logger](
      config:         Config = ConfigFactory.load(),
      maybeHistogram: Option[Histogram[F]] = None
  ): F[ExecutionTimeRecorder[F]] = for {
    duration <- find[F, FiniteDuration]("logging.elapsed-time-threshold", config)
  } yield new ExecutionTimeRecorderImpl(ElapsedTime(duration), maybeHistogram)

  class ElapsedTime private (val value: Long) extends AnyVal with LongTinyType
  object ElapsedTime extends TinyTypeFactory[ElapsedTime](new ElapsedTime(_)) {
    addConstraint(
      check = _ >= 0,
      message = (_: Long) => s"$typeName cannot be < 0"
    )

    def apply(elapsedTime: FiniteDuration): ElapsedTime = ElapsedTime(elapsedTime.toMillis)
  }
}
