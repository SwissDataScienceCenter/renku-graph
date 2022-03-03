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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.util.control.NonFatal

private trait EventStatusRenamer[F[_]] extends DbMigrator[F]

private case class EventStatusRenamerImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends EventStatusRenamer[F] {

  override def run(): F[Unit] = {
    for {
      _ <- renameAllStatuses(from = "PROCESSING", to = "GENERATING_TRIPLES")
      _ <- Logger[F].info(s"'PROCESSING' event status renamed to 'GENERATING_TRIPLES'")
      _ <- renameAllStatuses(from = "RECOVERABLE_FAILURE", to = "GENERATION_RECOVERABLE_FAILURE")
      _ <-
        Logger[F].info(s"'RECOVERABLE_FAILURE' event status renamed to 'GENERATION_RECOVERABLE_FAILURE'")
      _ <- renameAllStatuses(from = "NON_RECOVERABLE_FAILURE", to = "GENERATION_NON_RECOVERABLE_FAILURE")
      _ <- Logger[F].info(
             s"'NON_RECOVERABLE_FAILURE' event status renamed to 'GENERATION_NON_RECOVERABLE_FAILURE'"
           )
    } yield ()
  } recoverWith logging

  private def renameAllStatuses(from: String, to: String) = sessionResource.useK {
    val query: Command[String ~ String] = sql"""UPDATE event SET status = $varchar WHERE status = $varchar""".command
    Kleisli(_.prepare(query).use(_.execute(to ~ from)).void)
  }

  private lazy val logging: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Renaming of events failed")
    exception.raiseError[F, Unit]
  }
}

private object EventStatusRenamer {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): EventStatusRenamer[F] = EventStatusRenamerImpl(sessionResource)
}
