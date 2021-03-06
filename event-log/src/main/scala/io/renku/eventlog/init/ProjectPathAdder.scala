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
import cats.effect.{Bracket, BracketThrow}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.parser._
import io.circe.{Decoder, HCursor}
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.util.control.NonFatal

private trait ProjectPathAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object ProjectPathAdder {
  def apply[Interpretation[_]: BracketThrow](
      sessionResource: SessionResource[Interpretation, EventLogDB],
      logger:          Logger[Interpretation]
  ): ProjectPathAdder[Interpretation] =
    new ProjectPathAdderImpl(sessionResource, logger)
}

private class ProjectPathAdderImpl[Interpretation[_]: BracketThrow](
    sessionResource: SessionResource[Interpretation, EventLogDB],
    logger:          Logger[Interpretation]
) extends ProjectPathAdder[Interpretation]
    with EventTableCheck
    with TypeSerializers {

  override def run(): Interpretation[Unit] = sessionResource.useK {
    whenEventTableExists(
      Kleisli.liftF(logger info "'project_path' column adding skipped"),
      otherwise = checkColumnExists >>= {
        case true  => Kleisli.liftF(logger info "'project_path' column exists")
        case false => addColumn()
      }
    )
  }

  private lazy val checkColumnExists: Kleisli[Interpretation, Session[Interpretation], Boolean] = {
    val query: Query[Void, String] = sql"select project_path from event_log limit 1".query(varchar)
    Kleisli(
      _.option(query)
        .map(_ => true)
        .recover { case _ => false }
    )
  }

  private def addColumn(): Kleisli[Interpretation, Session[Interpretation], Unit] = {
    for {
      _                  <- execute(sql"ALTER TABLE event_log ADD COLUMN IF NOT EXISTS project_path VARCHAR".command)
      projectIdsAndPaths <- findDistinctProjects
      _                  <- updatePaths(projectIdsAndPaths)
      _                  <- execute(sql"ALTER TABLE event_log ALTER COLUMN project_path SET NOT NULL".command)
      _                  <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_path ON event_log(project_path)".command)
      _                  <- Kleisli.liftF(logger.info("'project_path' column added"))
    } yield ()
  } recoverWith logging

  private lazy val findDistinctProjects: Kleisli[Interpretation, Session[Interpretation], List[(Id, Path)]] = {

    val query: Query[Void, String] = sql"select min(event_body) from event_log group by project_id;".query(text)
    Kleisli(_.execute(query).flatMap(toListOfProjectIdAndPath))
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
  ): Kleisli[Interpretation, Session[Interpretation], Unit] =
    projectIdsAndPaths
      .map(toSqlUpdate)
      .sequence
      .map(_ => ())

  private lazy val toSqlUpdate: ((Id, Path)) => Kleisli[Interpretation, Session[Interpretation], Unit] = {
    case (projectId, projectPath) =>
      val query: Command[projects.Path ~ projects.Id] =
        sql"update event_log set project_path = $projectPathEncoder where project_id = $projectIdEncoder".command
      Kleisli(_.prepare(query).use(_.execute(projectPath ~ projectId)).void)
  }

  private lazy val logging: PartialFunction[Throwable, Kleisli[Interpretation, Session[Interpretation], Unit]] = {
    case NonFatal(exception) =>
      Kleisli.liftF(
        logger
          .error(exception)("'project_path' column adding failure")
          .flatMap(_ => exception.raiseError[Interpretation, Unit])
      )
  }
}
