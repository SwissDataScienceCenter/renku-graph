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

package io.renku.triplesstore

import cats.effect.Sync
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.LabeledHistogramImpl
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

class SparqlQueryTimeRecorder[F[_]](val instance: ExecutionTimeRecorder[F])

object SparqlQueryTimeRecorder {

  import io.renku.metrics.MetricsRegistry

  def create[F[_]: Sync: Logger: MetricsRegistry](): F[SparqlQueryTimeRecorder[F]] = MetricsRegistry[F]
    .register {
      new LabeledHistogramImpl[F](
        name = "sparql_execution_times",
        help = "Sparql execution times",
        labelName = "query_id",
        buckets = Seq(.05, .1, .5, 1, 2.5, 5, 10, 25, 50),
        maybeThreshold = (750 millis).some
      )
    }
    .flatMap(histogram => ExecutionTimeRecorder[F](maybeHistogram = Some(histogram)))
    .map(new SparqlQueryTimeRecorder(_))

  def apply[F[_]](implicit ev: SparqlQueryTimeRecorder[F]): SparqlQueryTimeRecorder[F] = ev
}
