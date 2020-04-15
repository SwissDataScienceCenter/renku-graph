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

package ch.datascience.dbeventlog.statuschange

import cats.effect.Bracket
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}

import scala.language.higherKinds

private class UpdateCommandsRunner[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  import cats.implicits._
  import doobie.implicits._

  def run(command: ChangeStatusCommand): Interpretation[UpdateResult] =
    for {
      queryResult  <- command.query.update.run transact transactor.get
      updateResult <- ME.catchNonFatal(command mapResult queryResult)
    } yield updateResult
}

private object IOUpdateCommandsRunner {

  import cats.effect.IO

  def apply(transactor: DbTransactor[IO, EventLogDB]): IO[UpdateCommandsRunner[IO]] = IO {
    new UpdateCommandsRunner[IO](transactor)
  }
}
