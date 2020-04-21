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

package ch.datascience.eventlog.statuschange

import cats.effect.Bracket
import ch.datascience.db.DbTransactor
import ch.datascience.eventlog.EventLogDB
import ch.datascience.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}

import scala.language.higherKinds

trait StatusUpdatesRunner[Interpretation[_]] {
  def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult]
}

class StatusUpdatesRunnerImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends StatusUpdatesRunner[Interpretation] {

  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  import cats.implicits._
  import doobie.implicits._

  override def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult] =
    for {
      queryResult  <- command.query.update.run transact transactor.get
      updateResult <- ME.catchNonFatal(command mapResult queryResult)
      _            <- command updateGauges updateResult
    } yield updateResult
}

object IOUpdateCommandsRunner {

  import cats.effect.IO

  def apply(transactor: DbTransactor[IO, EventLogDB]): IO[StatusUpdatesRunner[IO]] = IO {
    new StatusUpdatesRunnerImpl[IO](transactor)
  }
}
