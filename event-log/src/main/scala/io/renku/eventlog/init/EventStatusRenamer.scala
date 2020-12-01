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
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, Processing}
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.util.control.NonFatal

trait EventStatusRenamer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private case class EventStatusRenamerImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventStatusRenamer[Interpretation] {
  override def run(): Interpretation[Unit] = {
    for {
      _ <- renameAllStatuses(from = Processing, to = GeneratingTriples)
      _ <- logger.info(s"'$Processing' event status renamed to '$GeneratingTriples'")
    } yield ()
  } recoverWith logging

  private def renameAllStatuses(from: EventStatus, to: EventStatus) =
    sql"""UPDATE event SET status = ${to.toString} WHERE status = ${from.toString}""".update.run
      .transact(transactor.get)
      .void

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)(s"Renaming of $Processing event failed")
    ME.raiseError(exception)
  }
}

private object EventStatusRenamer {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventStatusRenamer[Interpretation] =
    new EventStatusRenamerImpl(transactor, logger)
}
