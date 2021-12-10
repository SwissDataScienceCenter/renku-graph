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

import cats.effect.kernel.Ref
import cats.effect.{IO, Temporal}
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.init.DbInitializer._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

trait DbInitializer[F[_]] {
  def run(): F[Unit]
}

class DbInitializerImpl[F[_]: Temporal: Logger](migrators: List[Runnable[F, Unit]],
                                                isMigrating:        Ref[F, Boolean],
                                                retrySleepDuration: FiniteDuration = 20.seconds
) extends DbInitializer[F] {

  override def run(): F[Unit] = runMigrations()

  private def runMigrations(retryCount: Int = 0): F[Unit] = {
    for {
      _ <- isMigrating.update(_ => true)
      _ <- migrators.map(_.run()).sequence
      _ <- Logger[F].info("Event Log database initialization success")
      _ <- isMigrating.update(_ => false)
    } yield ()

  } recoverWith logAndRetry(retryCount + 1)

  private def logAndRetry(retryCount: Int): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- Temporal[F] sleep retrySleepDuration
      _ <- Logger[F].error(exception)(
             s"Event Log database initialization failed: retrying $retryCount time(s)"
           )
      _ <- runMigrations(retryCount)
    } yield ()
  }
}

object DbInitializer {
  def apply(sessionResource: SessionResource[IO, EventLogDB], isMigrating: Ref[IO, Boolean])(implicit
      logger:                Logger[IO]
  ): IO[DbInitializer[IO]] = IO {
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
      ),
      isMigrating
    )
  }

  private[init] type Runnable[F[_], R] = { def run(): F[R] }
}
