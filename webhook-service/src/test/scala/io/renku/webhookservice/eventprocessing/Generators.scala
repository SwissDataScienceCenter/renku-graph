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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalacheck.{Arbitrary, Gen}

import scala.math.BigDecimal.RoundingMode

private object Generators {

  implicit val statusInfos: Gen[StatusInfo] =
    (Arbitrary.arbBool.arbitrary, nonZeroProgressStatuses).mapN(StatusInfo)

  implicit lazy val nonZeroProgressStatuses: Gen[ProgressStatus.NonZero] = for {
    total <- nonNegativeInts(max = Integer.MAX_VALUE)
    done  <- nonNegativeInts(max = total.value)
    progress = Refined.unsafeApply(
                 if (total.value == 0) 100d
                 else
                   BigDecimal((done.value.toDouble / total.value) * 100)
                     .setScale(2, RoundingMode.HALF_DOWN)
                     .toDouble
               ): ProgressStatus.Progress
  } yield ProgressStatus.NonZero(done, total, progress)

  implicit lazy val progressStatuses: Gen[ProgressStatus] =
    Gen.oneOf(nonZeroProgressStatuses, fixed(ProgressStatus.Zero))
}
