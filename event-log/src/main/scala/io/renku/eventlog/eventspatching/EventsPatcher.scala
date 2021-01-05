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

package io.renku.eventlog.eventspatching

import cats.effect.{Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.util.control.NonFatal

private trait EventsPatcher[Interpretation[_]] {
  def applyToAllEvents(eventsPatch: EventsPatch[Interpretation]): Interpretation[Unit]
}

private class EventsPatcherImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    logger:           Logger[IO]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventsPatcher[IO] {

  def applyToAllEvents(eventsPatch: EventsPatch[IO]): IO[Unit] = {
    for {
      _ <- measureExecutionTime(eventsPatch.query) transact transactor.get
      _ <- eventsPatch.updateGauges()
      _ <- logger.info(s"All events patched with ${eventsPatch.name}")
    } yield ()
  } recoverWith loggedError(eventsPatch)

  private def loggedError(patch: EventsPatch[IO]): PartialFunction[Throwable, IO[Unit]] = { case NonFatal(exception) =>
    val message = s"Patching all events with ${patch.name} failed"
    logger.error(exception)(message)
    ME.raiseError {
      new Exception(message, exception)
    }
  }
}

private object IOEventsPatcher {
  def apply(
      transactor:       DbTransactor[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
      logger:           Logger[IO]
  ): IO[EventsPatcher[IO]] = IO {
    new EventsPatcherImpl(transactor, queriesExecTimes, logger)
  }
}
