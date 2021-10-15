/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.Generators._
import eu.timepit.refined.api.Refined
import io.renku.webhookservice.eventprocessing.ProcessingStatusFetcher.ProcessingStatus
import org.scalacheck.Gen

import scala.math.BigDecimal.RoundingMode

private object ProcessingStatusGenerator {

  implicit val processingStatuses: Gen[ProcessingStatus] = for {
    total <- nonNegativeInts(max = Integer.MAX_VALUE)
    done  <- nonNegativeInts(max = total.value)
    progress = Refined.unsafeApply(
                 if (total.value == 0) 100d
                 else
                   BigDecimal((done.value.toDouble / total.value) * 100)
                     .setScale(2, RoundingMode.HALF_DOWN)
                     .toDouble
               ): ProcessingStatus.Progress
  } yield ProcessingStatus(done, total, progress)
}
