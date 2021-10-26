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

package io.renku.graph.acceptancetests.db

import cats.Applicative
import cats.data.{Kleisli, NonEmptyList}
import cats.effect.IO._
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import io.renku.db.{DBConfigProvider, SessionResource}
import io.renku.eventlog._
import io.renku.graph.model.events.{CommitId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

import scala.collection.immutable
import scala.util.Try

object EventLog extends TypeSerializers {

  def findEvents(projectId: Id)(implicit ioRuntime: IORuntime): List[(EventId, EventStatus)] = execute { session =>
    val query: Query[projects.Id, (EventId, EventStatus)] =
      sql"""SELECT event_id, status
            FROM event
            WHERE project_id = $projectIdEncoder"""
        .query(eventIdDecoder ~ eventStatusDecoder)
        .map { case id ~ status => (id, status) }
    session.prepare(query).use(_.stream(projectId, 32).compile.toList)
  }

  def findEvents(projectId: Id, status: EventStatus*)(implicit ioRuntime: IORuntime): List[CommitId] = execute {
    session =>
      val query: Query[projects.Id, CommitId] =
        sql"""SELECT event_id
            FROM event
            WHERE project_id = $projectIdEncoder AND #${`status IN`(status.toList)}"""
          .query(eventIdDecoder)
          .map(eventId => CommitId(eventId.value))
      session.prepare(query).use(_.stream(projectId, 32).compile.toList)
  }

  private def `status IN`(status: List[EventStatus]) =
    s"status IN (${NonEmptyList.fromListUnsafe(status).map(el => s"'$el'").toList.mkString(",")})"

  def execute[O](query: Session[IO] => IO[O])(implicit ioRuntime: IORuntime): O =
    sessionResource
      .use(_.useK(Kleisli[IO, Session[IO], O](session => query(session))))
      .unsafeRunSync()

  private lazy val dbConfig: DBConfigProvider.DBConfig[EventLogDB] =
    new EventLogDbConfigProvider[Try].get().fold(throw _, identity)

  private lazy val postgresContainer = FixedHostPortGenericContainer(
    imageName = "postgres:11.11-alpine",
    env = immutable.Map("POSTGRES_USER"     -> dbConfig.user.value,
                        "POSTGRES_PASSWORD" -> dbConfig.pass.value,
                        "POSTGRES_DB"       -> dbConfig.name.value
    ),
    exposedPorts = Seq(dbConfig.port.value),
    exposedHostPort = dbConfig.port.value,
    exposedContainerPort = dbConfig.port.value,
    command = Seq(s"-p ${dbConfig.port.value}")
  )

  def startDB()(implicit ioRuntime: IORuntime, logger: Logger[IO]): IO[Unit] = for {
    _ <- Applicative[IO].unlessA(postgresContainer.container.isRunning)(IO(postgresContainer.start()))
    _ <- logger.info("event_log DB started")
  } yield ()

  private lazy val sessionResource: Resource[IO, SessionResource[IO, EventLogDB]] =
    Session
      .pooled(
        host = postgresContainer.host,
        port = dbConfig.port.value,
        database = dbConfig.name.value,
        user = dbConfig.user.value,
        password = Some(dbConfig.pass.value),
        max = dbConfig.connectionPool.value
      )
      .map(new SessionResource(_))
}
