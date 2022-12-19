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

package io.renku.webhookservice.eventprocessing

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Encoder
import io.circe.literal._

import scala.math.BigDecimal.RoundingMode

private final case class StatusInfo(activated: Boolean, progress: ProgressStatus)

private trait ProgressStatus extends Product with Serializable

private object ProgressStatus {

  type Done     = Int Refined NonNegative
  type Total    = Int Refined NonNegative
  type Progress = Double Refined NonNegative

  final case class NonZero private (
      done:     Done,
      total:    Total,
      progress: Progress
  ) extends ProgressStatus
  final case object Zero extends ProgressStatus

  def from(done: Int, total: Int, progress: Double): Either[String, ProgressStatus.NonZero] = {
    import eu.timepit.refined.api.RefType.applyRef

    for {
      validDone     <- applyRef[Done](done).leftMap(_ => "ProcessingStatus's 'done' cannot be negative")
      validTotal    <- applyRef[Total](total).leftMap(_ => "ProcessingStatus's 'total' cannot be negative")
      validProgress <- applyRef[Progress](progress).leftMap(_ => "ProcessingStatus's 'progress' cannot be negative")
      _             <- if (done <= total) Right(()) else Left("ProcessingStatus's 'done' > 'total'")
      expectedProgress = BigDecimal((done.toDouble / total) * 100).setScale(2, RoundingMode.HALF_DOWN).toDouble
      _ <- if (expectedProgress == progress) Right(()) else Left("ProcessingStatus's 'progress' is invalid")
    } yield NonZero(validDone, validTotal, validProgress)
  }

  implicit def processingStatusEncoder[PS <: ProgressStatus]: Encoder[PS] = {
    case NonZero(done, total, progress) => json"""{
        "done":     ${done.value},
        "total":    ${total.value},
        "progress": ${progress.value}
      }"""
    case Zero => json"""{
       "done":  0,
       "total": 0
      }"""
  }
}
