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

import cats.effect.Bracket
import cats.syntax.all._
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import doobie.implicits._
import org.typelevel.log4cats.Logger
import io.circe.parser._
import io.circe.{Decoder, HCursor}
import io.renku.eventlog.EventLogDB

import scala.util.control.NonFatal

private trait ProjectPathAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object ProjectPathAdder {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): ProjectPathAdder[Interpretation] =
    new ProjectPathAdderImpl(transactor, logger)
}

private class ProjectPathAdderImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends ProjectPathAdder[Interpretation]
    with EventTableCheck[Interpretation] {

  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    whenEventTableExists(
      logger info "'project_path' column adding skipped",
      otherwise = checkColumnExists flatMap {
        case true  => logger info "'project_path' column exists"
        case false => addColumn()
      }
    )

  private def checkColumnExists: Interpretation[Boolean] =
    sql"select project_path from event_log limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }

  private def addColumn() = {
    for {
      _                  <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS project_path VARCHAR")
      projectIdsAndPaths <- findDistinctProjects
      _                  <- updatePaths(projectIdsAndPaths)
      _                  <- execute(sql"ALTER TABLE event_log ALTER COLUMN project_path SET NOT NULL")
      _                  <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON event_log(project_path)")
      _                  <- logger.info("'project_path' column added")
    } yield ()
  } recoverWith logging

  private def findDistinctProjects: Interpretation[List[(Id, Path)]] =
    sql"select min(event_body) from event_log group by project_id;"
      .query[String]
      .to[List]
      .transact(transactor.get)
      .flatMap(toListOfProjectIdAndPath)

  private def toListOfProjectIdAndPath(bodies: List[String]): Interpretation[List[(Id, Path)]] =
    bodies.map(parseToProjectIdAndPath).sequence

  private def parseToProjectIdAndPath(body: String): Interpretation[(Id, Path)] = ME.fromEither {
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

  private def updatePaths(projectIdsAndPaths: List[(Id, Path)]): Interpretation[Unit] =
    projectIdsAndPaths
      .map(toSqlUpdate)
      .sequence
      .map(_ => ())

  private def toSqlUpdate: ((Id, Path)) => Interpretation[Unit] = { case (projectId, projectPath) =>
    sql"update event_log set project_path = ${projectPath.value} where project_id = ${projectId.value}".update.run
      .transact(transactor.get)
      .map(_ => ())
  }

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("'project_path' column adding failure")
    ME.raiseError(exception)
  }
}
