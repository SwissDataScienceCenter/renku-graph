/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.effect.{Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.util.control.NonFatal

trait DbInitializer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class DbInitializerImpl[Interpretation[_]](
    eventLogTableCreator:           EventLogTableCreator[Interpretation],
    eventPayloadTableCreator:       EventPayloadTableCreator[Interpretation],
    projectPathAdder:               ProjectPathAdder[Interpretation],
    batchDateAdder:                 BatchDateAdder[Interpretation],
    latestEventDatesViewRemover:    LatestEventDatesViewRemover[Interpretation],
    projectTableCreator:            ProjectTableCreator[Interpretation],
    projectPathRemover:             ProjectPathRemover[Interpretation],
    eventLogTableRenamer:           EventLogTableRenamer[Interpretation],
    eventStatusRenamer:             EventStatusRenamer[Interpretation],
    eventPayloadSchemaVersionAdder: EventPayloadSchemaVersionAdder[Interpretation],
    logger:                         Logger[Interpretation]
)(implicit ME:                      Bracket[Interpretation, Throwable])
    extends DbInitializer[Interpretation] {

  override def run(): Interpretation[Unit] = {
    for {
      _ <- eventLogTableCreator.run()
      _ <- projectPathAdder.run()
      _ <- batchDateAdder.run()
      _ <- latestEventDatesViewRemover.run()
      _ <- projectTableCreator.run()
      _ <- projectPathRemover.run()
      _ <- eventLogTableRenamer.run()
      _ <- eventStatusRenamer.run()
      _ <- eventPayloadTableCreator.run()
      _ <- eventPayloadSchemaVersionAdder.run()
      _ <- logger info "Event Log database initialization success"
    } yield ()
  } recoverWith logging

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("Event Log database initialization failure")
    exception.raiseError[Interpretation, Unit]
  }
}

object IODbInitializer {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[DbInitializer[IO]] = IO {
    new DbInitializerImpl[IO](
      EventLogTableCreator(transactor, logger),
      EventPayloadTableCreator(transactor, logger),
      ProjectPathAdder(transactor, logger),
      BatchDateAdder(transactor, logger),
      LatestEventDatesViewRemover[IO](transactor, logger),
      ProjectTableCreator(transactor, logger),
      ProjectPathRemover(transactor, logger),
      EventLogTableRenamer(transactor, logger),
      EventStatusRenamer(transactor, logger),
      EventPayloadSchemaVersionAdder(transactor, logger),
      logger
    )
  }
}
