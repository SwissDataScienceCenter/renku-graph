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
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait ProjectPathRemover[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object ProjectPathRemover {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): ProjectPathRemover[Interpretation] =
    new ProjectPathRemoverImpl(transactor, logger)
}

private class ProjectPathRemoverImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends ProjectPathRemover[Interpretation]
    with EventTableCheck[Interpretation] {

  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    whenEventTableExists(
      logger info "'project_path' column dropping skipped",
      otherwise = checkColumnExists flatMap {
        case false => logger info "'project_path' column already removed"
        case true  => removeColumn()
      }
    )

  private def checkColumnExists: Interpretation[Boolean] =
    sql"select project_path from event_log limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }

  private def removeColumn() = for {
    _ <- execute(sql"ALTER TABLE event_log DROP COLUMN IF EXISTS project_path")
    _ <- logger info "'project_path' column removed"
  } yield ()
}
