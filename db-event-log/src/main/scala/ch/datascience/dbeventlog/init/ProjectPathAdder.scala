/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.init

import cats.effect.Bracket
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.graph.model.events.{ProjectId, ProjectPath}
import doobie.implicits._
import doobie.util.fragment.Fragment
import io.chrisdavenport.log4cats.Logger
import io.circe.parser._
import io.circe.{Decoder, HCursor}

import scala.language.higherKinds
import scala.util.control.NonFatal

private class ProjectPathAdder[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def run: Interpretation[Unit] =
    checkColumnExists flatMap {
      case true  => logger.info("'project_path' column exists")
      case false => addColumn
    }

  private def checkColumnExists: Interpretation[Boolean] =
    sql"select project_path from event_log limit 1"
      .query[String]
      .option
      .transact(transactor.get)
      .map(_ => true)
      .recover { case _ => false }

  private def addColumn = {
    for {
      _                  <- execute(sql"ALTER TABLE event_log ADD COLUMN project_path VARCHAR", transactor)
      projectIdsAndPaths <- findDistinctProjects
      _                  <- updatePaths(projectIdsAndPaths)
      _                  <- execute(sql"ALTER TABLE event_log ALTER COLUMN project_path SET NOT NULL", transactor)
      _                  <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON event_log(project_path)", transactor)
      _                  <- logger.info("'project_path' column added")
    } yield ()
  } recoverWith logging

  private def findDistinctProjects: Interpretation[List[(ProjectId, ProjectPath)]] =
    sql"select event_body from event_log group by project_id"
      .query[String]
      .to[List]
      .transact(transactor.get)
      .flatMap(toListOfProjectIdAndPath)

  private def toListOfProjectIdAndPath(bodies: List[String]): Interpretation[List[(ProjectId, ProjectPath)]] =
    bodies.map(parseToProjectIdAndPath).sequence

  private def parseToProjectIdAndPath(body: String): Interpretation[(ProjectId, ProjectPath)] = ME.fromEither {
    for {
      json  <- parse(body)
      tuple <- json.as[(ProjectId, ProjectPath)]
    } yield tuple
  }

  private implicit lazy val projectInfoDecoder: Decoder[(ProjectId, ProjectPath)] = (cursor: HCursor) =>
    for {
      id   <- cursor.downField("project").downField("id").as[ProjectId]
      path <- cursor.downField("project").downField("path").as[ProjectPath]
    } yield id -> path

  private def updatePaths(projectIdsAndPaths: List[(ProjectId, ProjectPath)]): Interpretation[Unit] =
    projectIdsAndPaths
      .map(toSqlUpdate)
      .sequence
      .map(_ => ())

  private def toSqlUpdate: ((ProjectId, ProjectPath)) => Interpretation[Unit] = {
    case (projectId, projectPath) =>
      sql"update event_log set project_path = ${projectPath.value} where project_id = ${projectId.value}".update.run
        .transact(transactor.get)
        .map(_ => ())
  }

  private def execute(sql: Fragment, transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] =
    sql.update.run
      .transact(transactor.get)
      .map(_ => ())

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)("'project_path' column adding failure")
      ME.raiseError(exception)
  }
}
