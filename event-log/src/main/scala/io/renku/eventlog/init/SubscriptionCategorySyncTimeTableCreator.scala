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
import cats.effect.BracketThrow
import ch.datascience.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait SubscriptionCategorySyncTimeTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object SubscriptionCategorySyncTimeTableCreator {
  def apply[Interpretation[_]: BracketThrow](
      sessionResource: SessionResource[Interpretation, EventLogDB],
      logger:          Logger[Interpretation]
  ): SubscriptionCategorySyncTimeTableCreator[Interpretation] =
    new SubscriptionCategorySyncTimeTableCreatorImpl[Interpretation](sessionResource, logger)
}

private class SubscriptionCategorySyncTimeTableCreatorImpl[Interpretation[_]: BracketThrow](
    sessionResource: SessionResource[Interpretation, EventLogDB],
    logger:          Logger[Interpretation]
) extends SubscriptionCategorySyncTimeTableCreator[Interpretation] {

  import cats.syntax.all._

  override def run(): Interpretation[Unit] = sessionResource.useK {
    checkTableExists >>= {
      case true  => Kleisli.liftF(logger info "'subscription_category_sync_time' table exists")
      case false => createTable()
    }
  }

  private lazy val checkTableExists: Kleisli[Interpretation, Session[Interpretation], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscription_category_sync_time')".query(bool)
    Kleisli(
      _.unique(query)
        .recover { case _ => false }
    )
  }

  private def createTable(): Kleisli[Interpretation, Session[Interpretation], Unit] =
    for {
      _ <- execute(createTableSql)
      _ <- execute(
             sql"CREATE INDEX IF NOT EXISTS idx_project_id       ON subscription_category_sync_time(project_id)".command
           )
      _ <-
        execute(
          sql"CREATE INDEX IF NOT EXISTS idx_category_name    ON subscription_category_sync_time(category_name)".command
        )
      _ <-
        execute(
          sql"CREATE INDEX IF NOT EXISTS idx_last_synced      ON subscription_category_sync_time(last_synced)".command
        )
      _ <- Kleisli.liftF(logger info "'subscription_category_sync_time' table created")
      _ <- execute(foreignKeySql)
    } yield ()

  private lazy val createTableSql: Command[Void] =
    sql"""
    CREATE TABLE IF NOT EXISTS subscription_category_sync_time(
      project_id        int4      NOT NULL,
      category_name     VARCHAR   NOT NULL,
      last_synced       timestamp NOT NULL,
      PRIMARY KEY (project_id, category_name)
    );
    """.command

  private lazy val foreignKeySql: Command[Void] =
    sql"""
    ALTER TABLE subscription_category_sync_time
    ADD CONSTRAINT fk_project
    FOREIGN KEY (project_id) 
    REFERENCES project (project_id)
  """.command
}
