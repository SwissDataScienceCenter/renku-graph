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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.BracketThrow
import cats.syntax.all._
import ch.datascience.db.SessionResource
import io.renku.eventlog.EventLogDB
import skunk.Transaction

import scala.util.control.NonFatal

private trait StatusChanger[Interpretation[_]] {
  def updateStatuses[E <: StatusChangeEvent](event: E)(implicit
      dbUpdater:                                    DBUpdater[Interpretation, E]
  ): Interpretation[Unit]
}

private class StatusChangerImpl[Interpretation[_]: BracketThrow](
    sessionResource: SessionResource[Interpretation, EventLogDB],
    gaugesUpdater:   GaugesUpdater[Interpretation]
) extends StatusChanger[Interpretation] {

  import gaugesUpdater._

  override def updateStatuses[E <: StatusChangeEvent](
      event:            E
  )(implicit dbUpdater: DBUpdater[Interpretation, E]): Interpretation[Unit] = sessionResource.useWithTransactionK {
    Kleisli { case (transaction, session) =>
      {
        for {
          savepoint     <- Kleisli.liftF(transaction.savepoint)
          updateResults <- dbUpdater.updateDB(event) recoverWith rollback(transaction)(savepoint)(event)
          _             <- Kleisli.liftF(updateGauges(updateResults)) recoverWith { case NonFatal(_) => Kleisli.pure(()) }
        } yield ()
      } run session
    }
  }

  private def rollback[E <: StatusChangeEvent](transaction: Transaction[Interpretation])(
      savepoint:                                            transaction.Savepoint
  )(event:                                                  E)(implicit
      dbUpdater:                                            DBUpdater[Interpretation, E]
  ): PartialFunction[Throwable, UpdateResult[Interpretation]] = { case NonFatal(err) =>
    Kleisli.liftF {
      for {
        _ <- transaction.rollback(savepoint)
        _ <- sessionResource.useK(dbUpdater onRollback event)
        _ <- err.raiseError[Interpretation, DBUpdateResults]
      } yield DBUpdateResults.ForProjects.empty
    }
  }
}
