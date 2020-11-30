/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.init

import cats.effect.Bracket
import cats.syntax.all._
import ch.datascience.db.DbTransactor
import doobie.implicits._
import io.renku.eventlog.EventLogDB

private trait EventTableCheck[Interpretation[_]] {

  def whenEventTableExists(
      eventTableExistsMessage: => Interpretation[Unit],
      otherwise:               => Interpretation[Unit]
  )(implicit
      transactor: DbTransactor[Interpretation, EventLogDB],
      ME:         Bracket[Interpretation, Throwable]
  ): Interpretation[Unit] = checkTableExists flatMap {
    case true  => eventTableExistsMessage
    case false => otherwise
  }

  private def checkTableExists(implicit
      transactor: DbTransactor[Interpretation, EventLogDB],
      ME:         Bracket[Interpretation, Throwable]
  ): Interpretation[Boolean] =
    sql"select event_id from event limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }
}
