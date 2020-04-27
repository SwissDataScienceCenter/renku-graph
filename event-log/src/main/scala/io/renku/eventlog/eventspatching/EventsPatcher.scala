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

package io.renku.eventlog.eventspatching

import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import doobie.implicits._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.language.higherKinds
import scala.util.control.NonFatal

private class EventsPatcher[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def applyToAllEvents(eventsPatch: EventsPatch[Interpretation]): Interpretation[Unit] = {
    for {
      _ <- eventsPatch.query.update.run transact transactor.get
      _ <- eventsPatch.updateGauges()
      _ <- logger.info(s"All events patched with ${eventsPatch.name}")
    } yield ()
  } recoverWith loggedError(eventsPatch)

  private def loggedError(patch: EventsPatch[Interpretation]): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      val message = s"Patching all events with ${patch.name} failed"
      logger.error(exception)(message)
      ME.raiseError {
        new Exception(message, exception)
      }
  }
}

private class IOEventsPatcher(
    transactor:          DbTransactor[IO, EventLogDB],
    logger:              Logger[IO]
)(implicit contextShift: ContextShift[IO])
    extends EventsPatcher[IO](transactor, logger)
