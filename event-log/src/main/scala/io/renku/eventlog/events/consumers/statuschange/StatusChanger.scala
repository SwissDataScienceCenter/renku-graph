/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import skunk.Transaction

import scala.util.control.NonFatal

private trait StatusChanger[F[_]] {
  def updateStatuses[E <: StatusChangeEvent](event: E)(implicit dbUpdater: DBUpdater[F, E]): F[Unit]
}

private class StatusChangerImpl[F[_]: MonadCancelThrow: SessionResource](gaugesUpdater: GaugesUpdater[F])
    extends StatusChanger[F] {

  import gaugesUpdater._

  override def updateStatuses[E <: StatusChangeEvent](event: E)(implicit dbUpdater: DBUpdater[F, E]): F[Unit] =
    SessionResource[F].useWithTransactionK {
      Kleisli { case (transaction, session) =>
        {
          for {
            savepoint <- Kleisli.liftF(transaction.savepoint)
            updateResults <-
              dbUpdater
                .updateDB(event)
                .flatMapF(res => transaction.commit.map(_ => res))
                .recoverWith(rollback(transaction)(savepoint)(event))
            _ <- Kleisli.liftF(updateGauges(updateResults)) recoverWith { case NonFatal(_) => Kleisli.pure(()) }
          } yield ()
        } run session
      }
    }

  private def rollback[E <: StatusChangeEvent](transaction: Transaction[F])(savepoint: transaction.Savepoint)(event: E)(
      implicit dbUpdater:                                   DBUpdater[F, E]
  ): PartialFunction[Throwable, UpdateResult[F]] = { case NonFatal(err) =>
    Kleisli.liftF {
      for {
        _ <- transaction.rollback(savepoint)
        _ <- SessionResource[F].useK(dbUpdater onRollback event)
        _ <- err.raiseError[F, DBUpdateResults]
      } yield DBUpdateResults.ForProjects.empty
    }
  }
}
