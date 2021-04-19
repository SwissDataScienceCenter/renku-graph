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

import cats.effect.Bracket
import ch.datascience.db.SessionResource
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait SubscriptionCategorySyncTimeTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object SubscriptionCategorySyncTimeTableCreator {
  def apply[Interpretation[_]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): SubscriptionCategorySyncTimeTableCreator[Interpretation] =
    new SubscriptionCategorySyncTimeTableCreatorImpl[Interpretation](transactor, logger)
}

private class SubscriptionCategorySyncTimeTableCreatorImpl[Interpretation[_]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends SubscriptionCategorySyncTimeTableCreator[Interpretation] {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true => logger info "'subscription_category_sync_time' table exists"
      case false =>
        transactor.use { implicit session =>
          session.transaction.use { xa =>
            for {
              sp <- xa.savepoint
              _ <- createTable recoverWith { e =>
                     xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
                   }
            } yield ()
          }
        }
    }

  private def checkTableExists: Interpretation[Boolean] =
    transactor.use { session =>
      val query: Query[Void, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscription_category_sync_time')".query(bool)
      session
        .unique(query)
        .recover { case _ => false }
    }

  private def createTable(implicit session: Session[Interpretation]) = for {
    _ <- execute(createTableSql)
    _ <- execute(
           sql"CREATE INDEX IF NOT EXISTS idx_project_id       ON subscription_category_sync_time(project_id)".command
         )
    _ <-
      execute(
        sql"CREATE INDEX IF NOT EXISTS idx_category_name    ON subscription_category_sync_time(category_name)".command
      )
    _ <- execute(
           sql"CREATE INDEX IF NOT EXISTS idx_last_synced      ON subscription_category_sync_time(last_synced)".command
         )
    _ <- logger info "'subscription_category_sync_time' table created"
    _ <- execute(foreignKeySql)
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS subscription_category_sync_time(
      project_id        int4      NOT NULL,
      category_name     VARCHAR   NOT NULL,
      last_synced       timestamp NOT NULL,
      PRIMARY KEY (project_id, category_name)
    );
    """.command

  private lazy val foreignKeySql: Command[Void] = sql"""
    ALTER TABLE subscription_category_sync_time
    ADD CONSTRAINT fk_project
    FOREIGN KEY (project_id) 
    REFERENCES project (project_id)
  """.command
}
