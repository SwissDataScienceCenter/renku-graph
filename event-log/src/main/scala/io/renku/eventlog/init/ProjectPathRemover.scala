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

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

private trait ProjectPathRemover[F[_]] {
  def run(): F[Unit]
}

private object ProjectPathRemover {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): ProjectPathRemover[F] = new ProjectPathRemoverImpl(sessionResource)
}

private class ProjectPathRemoverImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends ProjectPathRemover[F]
    with EventTableCheck {

  override def run(): F[Unit] = sessionResource.useK {
    whenEventTableExists(
      Kleisli.liftF(Logger[F] info "'project_path' column dropping skipped"),
      otherwise = checkColumnExists >>= {
        case false =>
          Kleisli.liftF[F, Session[F], Unit](
            Logger[F] info "'project_path' column already removed"
          )
        case true => removeColumn()
      }
    )
  }

  private lazy val checkColumnExists = {
    val query: Query[Void, String] = sql"select project_path from event_log limit 1".query(varchar)
    Kleisli[F, Session[F], Boolean] {
      _.option(query)
        .map(_ => true)
        .recover { case _ => false }
    }
  }

  private def removeColumn(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(sql"ALTER TABLE event_log DROP COLUMN IF EXISTS project_path".command)
    _ <- Kleisli.liftF(Logger[F] info "'project_path' column removed")
  } yield ()
}
