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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

private trait ProjectIdOnCleanUpTable[F[_]] extends DbMigrator[F]

private object ProjectIdOnCleanUpTable {
  def apply[F[_]: MonadCancelThrow: Logger: SessionResource]: ProjectIdOnCleanUpTable[F] =
    new ProjectIdOnCleanUpTableImpl[F]
}

private class ProjectIdOnCleanUpTableImpl[F[_]: MonadCancelThrow: Logger: SessionResource]
    extends ProjectIdOnCleanUpTable[F] {

  import MigratorTools._

  override def run(): F[Unit] = SessionResource[F].useK {
    checkColumnExists("clean_up_events_queue", "project_id") >>= {
      case true  => Kleisli.liftF(Logger[F] info "'clean_up_events_queue.project_id' column exists")
      case false => addColumn()
    }
  }

  private def addColumn(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(sql"ALTER TABLE clean_up_events_queue ADD COLUMN IF NOT EXISTS project_id INT4".command)
    _ <- addProjectIds()
    _ <- deleteNoProjectIdRows()
    _ <- execute(sql"ALTER TABLE clean_up_events_queue ALTER COLUMN project_id SET NOT NULL".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id ON clean_up_events_queue(project_id)".command)
    _ <- Kleisli.liftF(Logger[F].info("'clean_up_events_queue.project_id' column added"))
  } yield ()

  private def addProjectIds(): Kleisli[F, Session[F], Unit] = execute {
    sql"""UPDATE clean_up_events_queue
          SET project_id = proj.project_id
          FROM (
            SELECT p.project_id, p.project_path
            FROM clean_up_events_queue q
            JOIN project p ON p.project_path = q.project_path
          ) AS proj
          WHERE clean_up_events_queue.project_path = proj.project_path""".command
  }

  private def deleteNoProjectIdRows(): Kleisli[F, Session[F], Unit] = execute {
    sql"""DELETE FROM clean_up_events_queue
          WHERE project_id IS NULL""".command
  }
}
