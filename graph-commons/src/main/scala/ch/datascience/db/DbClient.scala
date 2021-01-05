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

package ch.datascience.db

import cats.effect.{Async, IO}
import ch.datascience.db.SqlQuery.Name
import ch.datascience.metrics.LabeledHistogram
import doobie.free.connection.ConnectionIO
import doobie.implicits._

abstract class DbClient(maybeHistogram: Option[LabeledHistogram[IO, Name]]) {

  protected def measureExecutionTime[ResultType](query: SqlQuery[ResultType]): ConnectionIO[ResultType] =
    maybeHistogram match {
      case None => query.query
      case Some(histogram) =>
        for {
          timer  <- Async[ConnectionIO].liftIO(histogram startTimer query.name)
          result <- query.query
          _      <- Async[ConnectionIO].liftIO(timer.observeDuration)
        } yield result
    }

}
