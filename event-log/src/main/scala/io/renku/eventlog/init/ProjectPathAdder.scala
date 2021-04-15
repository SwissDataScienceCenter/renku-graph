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

import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.chrisdavenport.log4cats.Logger
import io.circe.parser._
import io.circe.{Decoder, HCursor}
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.data.Completion

import scala.util.control.NonFatal

private trait ProjectPathAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object ProjectPathAdder {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): ProjectPathAdder[Interpretation] =
    new ProjectPathAdderImpl(transactor, logger)
}

private class ProjectPathAdderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends ProjectPathAdder[Interpretation]
    with EventTableCheck
    with TypeSerializers {

  override def run(): Interpretation[Unit] = transactor.use { implicit session =>
    whenEventTableExists(
      logger info "'project_path' column adding skipped",
      otherwise = checkColumnExists flatMap {
        case true => logger info "'project_path' column exists"
        case false =>
          session.transaction.use { xa =>
            for {
              sp <- xa.savepoint
              _ <- addColumn recoverWith { e =>
                     xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
                   }
            } yield ()
          }
      }
    )
  }

  private def checkColumnExists: Interpretation[Boolean] = transactor.use { session =>
    val query: Query[Void, String] = sql"select project_path from event_log limit 1".query(varchar)
    session
      .option(query)
      .map(_ => true)
      .recover { case _ => false }
  }

  private def addColumn(implicit session: Session[Interpretation]) = {
    for {
      _                  <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS project_path VARCHAR".command)
      projectIdsAndPaths <- findDistinctProjects
      _                  <- updatePaths(projectIdsAndPaths)
      _                  <- execute(sql"ALTER TABLE event_log ALTER COLUMN project_path SET NOT NULL".command)
      _                  <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON event_log(project_path)".command)
      _                  <- logger.info("'project_path' column added")
    } yield ()
  } recoverWith logging

  private def findDistinctProjects(implicit session: Session[Interpretation]): Interpretation[List[(Id, Path)]] = {

    val query: Query[Void, String] = sql"select min(event_body) from event_log group by project_id;".query(varchar)
    session.execute(query).flatMap(toListOfProjectIdAndPath)
  }

  private def toListOfProjectIdAndPath(bodies: List[String]): Interpretation[List[(Id, Path)]] =
    bodies.map(parseToProjectIdAndPath).sequence

  private def parseToProjectIdAndPath(body: String): Interpretation[(Id, Path)] =
    Bracket[Interpretation, Throwable].fromEither {
      for {
        json  <- parse(body)
        tuple <- json.as[(Id, Path)]
      } yield tuple
    }

  private implicit lazy val projectIdAndPathDecoder: Decoder[(Id, Path)] = (cursor: HCursor) =>
    for {
      id   <- cursor.downField("project").downField("id").as[Id]
      path <- cursor.downField("project").downField("path").as[Path]
    } yield id -> path

  private def updatePaths(
      projectIdsAndPaths: List[(Id, Path)]
  )(implicit session:     Session[Interpretation]): Interpretation[Unit] =
    projectIdsAndPaths
      .map(toSqlUpdate)
      .sequence
      .map(_ => ())

  private def toSqlUpdate(implicit session: Session[Interpretation]): ((Id, Path)) => Interpretation[Unit] = {
    case (projectId, projectPath) =>
      val query: Command[projects.Path ~ projects.Id] =
        sql"update event_log set project_path = $projectPathPut where project_id = $projectIdPut".command
      session.prepare(query).use(_.execute(projectPath ~ projectId)).void
  }

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger
      .error(exception)("'project_path' column adding failure")
      .flatMap(_ => exception.raiseError[Interpretation, Unit])
  }
}
