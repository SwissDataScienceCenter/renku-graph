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
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait SubscriptionCategorySyncTimeTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object SubscriptionCategorySyncTimeTableCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): SubscriptionCategorySyncTimeTableCreator[Interpretation] =
    new SubscriptionCategorySyncTimeTableCreatorImpl[Interpretation](transactor, logger)
}

private class SubscriptionCategorySyncTimeTableCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends SubscriptionCategorySyncTimeTableCreator[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => logger info "'subscription_category_sync_time' table exists"
      case false => createTable
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"select project_id from subscription_category_sync_time limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id       ON subscription_category_sync_time(project_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_category_name    ON subscription_category_sync_time(category_name)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_last_synced      ON subscription_category_sync_time(last_synced)")
    _ <- logger info "'subscription_category_sync_time' table created"
    _ <- foreignKeySql.run transact transactor.get
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS subscription_category_sync_time(
      project_id        int4      NOT NULL,
      category_name     VARCHAR   NOT NULL,
      last_synced       timestamp NOT NULL,
      PRIMARY KEY (project_id, category_name)
    );
    """.update

  private lazy val foreignKeySql = sql"""
    ALTER TABLE subscription_category_sync_time
    ADD CONSTRAINT fk_project
    FOREIGN KEY (project_id) 
    REFERENCES project (project_id)
  """.update
}
