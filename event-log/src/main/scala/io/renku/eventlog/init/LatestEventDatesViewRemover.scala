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

import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource}
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk.Command
import skunk.implicits.toStringOps

private[eventlog] trait LatestEventDatesViewRemover[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private[eventlog] object LatestEventDatesViewRemover {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): LatestEventDatesViewRemover[Interpretation] =
    new LatestEventDatesViewRemoverImpl(transactor, logger)
}

private[eventlog] class LatestEventDatesViewRemoverImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends DbClient[Interpretation](maybeHistogram = None)
    with LatestEventDatesViewRemover[Interpretation] {

  override def run(): Interpretation[Unit] = transactor.use { session =>
    for {
      _ <- session.execute(dropView)
      _ <- logger.info("'project_latest_event_date' view dropped")
    } yield ()
  }

  private lazy val dropView: Command[skunk.Void] =
    sql"""DROP MATERIALIZED VIEW IF EXISTS project_latest_event_date""".command
}
