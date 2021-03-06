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

import DbInitializer._
import cats.effect.{Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import org.typelevel.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.language.reflectiveCalls
import scala.util.control.NonFatal

trait DbInitializer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class DbInitializerImpl[Interpretation[_]](
    migrators: List[Runnable[Interpretation, Unit]],
    logger:    Logger[Interpretation]
)(implicit ME: Bracket[Interpretation, Throwable])
    extends DbInitializer[Interpretation] {

  override def run(): Interpretation[Unit] = {
    migrators.map(_.run()).sequence >> logger.info("Event Log database initialization success")
  } recoverWith logging

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger.error(exception)("Event Log database initialization failure")
    exception.raiseError[Interpretation, Unit]
  }
}

object DbInitializer {
  def apply(
      sessionResource:     SessionResource[IO, EventLogDB],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[DbInitializer[IO]] = IO {
    new DbInitializerImpl[IO](
      migrators = List[Runnable[IO, Unit]](
        EventLogTableCreator(sessionResource, logger),
        ProjectPathAdder(sessionResource, logger),
        BatchDateAdder(sessionResource, logger),
        ProjectTableCreator(sessionResource, logger),
        ProjectPathRemover(sessionResource, logger),
        EventLogTableRenamer(sessionResource, logger),
        EventStatusRenamer(sessionResource, logger),
        EventPayloadTableCreator(sessionResource, logger),
        EventPayloadSchemaVersionAdder(sessionResource, logger),
        SubscriptionCategorySyncTimeTableCreator(sessionResource, logger),
        StatusesProcessingTimeTableCreator(sessionResource, logger),
        SubscriberTableCreator(sessionResource, logger),
        EventDeliveryTableCreator(sessionResource, logger),
        TimestampZoneAdder(sessionResource, logger)
      ),
      logger
    )
  }

  private[init] type Runnable[F[_], R] = { def run(): F[R] }
}
