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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait ProjectPathRemover[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object ProjectPathRemover {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      sessionResource: SessionResource[Interpretation, EventLogDB],
      logger:          Logger[Interpretation]
  ): ProjectPathRemover[Interpretation] =
    new ProjectPathRemoverImpl(sessionResource, logger)
}

private class ProjectPathRemoverImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource: SessionResource[Interpretation, EventLogDB],
    logger:          Logger[Interpretation]
) extends ProjectPathRemover[Interpretation]
    with EventTableCheck {

  override def run(): Interpretation[Unit] = sessionResource.useK {
    whenEventTableExists(
      Kleisli.liftF(logger info "'project_path' column dropping skipped"),
      otherwise = checkColumnExists >>= {
        case false =>
          Kleisli.liftF[Interpretation, Session[Interpretation], Unit](
            logger info "'project_path' column already removed"
          )
        case true => removeColumn()
      }
    )
  }

  private lazy val checkColumnExists = {
    val query: Query[Void, String] = sql"select project_path from event_log limit 1".query(varchar)
    Kleisli[Interpretation, Session[Interpretation], Boolean] {
      _.option(query)
        .map(_ => true)
        .recover { case _ => false }
    }
  }

  private def removeColumn(): Kleisli[Interpretation, Session[Interpretation], Unit] =
    for {
      _ <- execute(sql"ALTER TABLE event_log DROP COLUMN IF EXISTS project_path".command)
      _ <- Kleisli.liftF(logger info "'project_path' column removed")
    } yield ()

}
