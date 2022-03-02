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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.tokenrepository.repository.ProjectsTokensDB
import org.typelevel.log4cats.Logger
import skunk.codec.all.bool

private trait ProjectsTokensTableCreator[F[_]] {
  def run(): F[Unit]
}

private object ProjectsTokensTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, ProjectsTokensDB]
  ): ProjectsTokensTableCreator[F] = new ProjectsTokensTableCreatorImpl(sessionResource)
}

private class ProjectsTokensTableCreatorImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, ProjectsTokensDB]
) extends ProjectsTokensTableCreator[F] {
  import skunk._
  import skunk.implicits._

  def run(): F[Unit] = sessionResource.useK {
    checkTableExists >>= {
      case false => createTable.flatMapF(_ => Logger[F].info("'projects_tokens' table created"))
      case true  => Kleisli.liftF(Logger[F].info("'projects_tokens' table existed"))
    }
  }

  private def checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[skunk.Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'projects_tokens')"
        .query(bool)
    Kleisli[F, Session[F], Boolean](_.unique(query).recover { case _ => false })
  }

  private def createTable: Kleisli[F, Session[F], Unit] = {
    val query: Command[Void] =
      sql"""CREATE TABLE IF NOT EXISTS projects_tokens(
              project_id int4 PRIMARY KEY,
              token VARCHAR NOT NULL
            );""".command
    Kleisli[F, Session[F], Unit](session => session.execute(query).void)
  }
}
