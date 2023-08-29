/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.UpdateOp
import io.renku.eventlog.metrics.EventStatusGauges
import org.typelevel.log4cats.Logger
import skunk.Transaction

private trait StatusChanger[F[_]] {
  def updateStatuses[E <: StatusChangeEvent](dbUpdater: DBUpdater[F, E])(event: E): F[Unit]
}

private object StatusChanger {
  def apply[F[_]: MonadCancelThrow: SessionResource: Logger: EventStatusGauges]: F[StatusChanger[F]] =
    MonadThrow[F].catchNonFatal(new StatusChangerImpl[F](GaugesUpdater[F]))
}

private[statuschange] class StatusChangerImpl[F[_]: MonadCancelThrow: SessionResource: Logger](
    gaugesUpdater: GaugesUpdater[F]
) extends StatusChanger[F] {

  import gaugesUpdater._

  override def updateStatuses[E <: StatusChangeEvent](dbUpdater: DBUpdater[F, E])(event: E): F[Unit] =
    SessionResource[F].useWithTransactionK {
      Kleisli { case (transaction, session) =>
        {
          for {
            savepoint <- Kleisli.liftF(transaction.savepoint)
            results <-
              dbUpdater
                .updateDB(event)
                .flatMapF(transaction.commit.as(_))
                .handleErrorWith(rollback(transaction)(savepoint)(event)(dbUpdater))
            _ <- Kleisli.liftF(updateGauges(results).handleErrorWith(Logger[F].error(_)("Updating gauges failed")))
          } yield ()
        } run session
      }
    }

  private def rollback[E <: StatusChangeEvent](transaction: Transaction[F])(savepoint: transaction.Savepoint)(event: E)(
      dbUpdater: DBUpdater[F, E]
  ): Throwable => UpdateOp[F] = { err =>
    def executeRollback: Throwable => F[DBUpdateResults] = { failure =>
      if ((dbUpdater onRollback event).isDefinedAt(failure))
        SessionResource[F]
          .useK((dbUpdater onRollback event).apply(failure))
          .handleErrorWith(executeRollback)
      else
        failure.raiseError[F, DBUpdateResults]
    }

    Kleisli.liftF {
      transaction.rollback(savepoint) >> executeRollback(err)
    }
  }
}
