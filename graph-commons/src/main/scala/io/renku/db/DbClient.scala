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

package io.renku.db

import cats.data.Kleisli
import cats.syntax.all._
import cats.{Monad, Show}
import io.renku.metrics.LabeledHistogram
import skunk.Session
import skunk.data.Completion

abstract class DbClient[F[_]: Monad](maybeHistogram: Option[LabeledHistogram[F]]) {

  protected def measureExecutionTime[ResultType](
      query: SqlStatement[F, ResultType]
  ): Kleisli[F, Session[F], ResultType] = Kleisli { session =>
    maybeHistogram match {
      case None => query.queryExecution.run(session)
      case Some(histogram) =>
        for {
          timer  <- histogram.startTimer(query.name.value)
          result <- query.queryExecution.run(session)
          _      <- timer.observeDuration
        } yield result
    }
  }

  protected implicit val completionShow: Show[Completion] = Show.show(c => c.toString)
}
