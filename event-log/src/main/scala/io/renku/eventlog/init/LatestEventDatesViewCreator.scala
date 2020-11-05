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
import ch.datascience.db.{DbClient, DbTransactor}
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private[eventlog] trait LatestEventDatesViewCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private[eventlog] object LatestEventDatesViewCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): LatestEventDatesViewCreator[Interpretation] =
    new LatestEventDatesViewCreatorImpl(transactor, logger)
}

private[eventlog] class LatestEventDatesViewCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends DbClient(maybeHistogram = None)
    with LatestEventDatesViewCreator[Interpretation] {

  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    for {
      _ <- createView.update.run transact transactor.get
      _ <- createViewIndex.update.run transact transactor.get
      _ <- logger.info("'project_latest_event_date' view created")
    } yield ()

  private lazy val createView = sql"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS project_latest_event_date AS
      select
        project_id,
        project_path,
        MAX(event_date) latest_event_date
      from event_log
      group by project_id, project_path
      order by latest_event_date desc;
    """

  private lazy val createViewIndex = sql"""
    CREATE UNIQUE INDEX IF NOT EXISTS project_latest_event_date_project_idx 
    ON project_latest_event_date (project_id, project_path)
    """
}
