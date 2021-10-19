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

import cats.effect.{IO, MonadCancelThrow}
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.init.DbInitializer._
import org.typelevel.log4cats.Logger

import scala.language.reflectiveCalls
import scala.util.control.NonFatal

trait DbInitializer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class DbInitializerImpl[Interpretation[_]: MonadCancelThrow: Logger](migrators: List[Runnable[Interpretation, Unit]])
    extends DbInitializer[Interpretation] {

  override def run(): Interpretation[Unit] = {
    migrators.map(_.run()).sequence >> Logger[Interpretation].info("Event Log database initialization success")
  } recoverWith logging

  private lazy val logging: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    Logger[Interpretation].error(exception)("Event Log database initialization failure")
    exception.raiseError[Interpretation, Unit]
  }
}

object DbInitializer {
  def apply(sessionResource: SessionResource[IO, EventLogDB])(implicit logger: Logger[IO]): IO[DbInitializer[IO]] = IO {
    new DbInitializerImpl[IO](
      migrators = List[Runnable[IO, Unit]](
        EventLogTableCreator(sessionResource),
        ProjectPathAdder(sessionResource),
        BatchDateAdder(sessionResource),
        ProjectTableCreator(sessionResource),
        ProjectPathRemover(sessionResource),
        EventLogTableRenamer(sessionResource),
        EventStatusRenamer(sessionResource),
        EventPayloadTableCreator(sessionResource),
        SubscriptionCategorySyncTimeTableCreator(sessionResource),
        StatusesProcessingTimeTableCreator(sessionResource),
        SubscriberTableCreator(sessionResource),
        EventDeliveryTableCreator(sessionResource),
        TimestampZoneAdder(sessionResource),
        PayloadTypeChanger(sessionResource)
      )
    )
  }

  private[init] type Runnable[F[_], R] = { def run(): F[R] }
}
