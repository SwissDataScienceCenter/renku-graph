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

import cats.data.Kleisli
import cats.effect.{Bracket, BracketThrow, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait EventsPatcher[Interpretation[_]] {
  def applyToAllEvents(eventsPatch: EventsPatch[Interpretation]): Interpretation[Unit]
}

private class EventsPatcherImpl[Interpretation[_]: BracketThrow](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    logger:           Logger[Interpretation]
) extends DbClient(Some(queriesExecTimes))
    with EventsPatcher[Interpretation] {

  def applyToAllEvents(eventsPatch: EventsPatch[Interpretation]): Interpretation[Unit] = sessionResource.useK {
    for {
      _ <- measureExecutionTime(eventsPatch.query)
      _ <- Kleisli.liftF(eventsPatch.updateGauges())
      _ <- Kleisli.liftF(logger.info(s"All events patched with ${eventsPatch.name}"))
    } yield ()
  } recoverWith loggedError(eventsPatch)

  private def loggedError(patch: EventsPatch[Interpretation]): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      val message = s"Patching all events with ${patch.name} failed"
      logger.error(exception)(message)
      Bracket[Interpretation, Throwable].raiseError {
        new Exception(message, exception)
      }
  }
}

private object IOEventsPatcher {
  def apply(
      sessionResource:  SessionResource[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name],
      logger:           Logger[IO]
  ): IO[EventsPatcher[IO]] = IO {
    new EventsPatcherImpl[IO](sessionResource, queriesExecTimes, logger)
  }
}
