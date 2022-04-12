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

package io.renku.eventlog.init

import cats.effect.kernel.Ref
import cats.effect.{MonadCancelThrow, Temporal}
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.graph.model.events.EventStatus._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

trait DbInitializer[F[_]] {
  def run(): F[Unit]
}

class DbInitializerImpl[F[_]: Temporal: Logger](migrators: List[DbMigrator[F]],
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
  def apply[F[_]: Temporal: Logger: SessionResource](isMigrating: Ref[F, Boolean]): F[DbInitializer[F]] =
    MonadCancelThrow[F].catchNonFatal {
      new DbInitializerImpl[F](
        migrators = List(
          EventLogTableCreator[F],
          ProjectPathAdder[F],
          BatchDateAdder[F],
          ProjectTableCreator[F],
          ProjectPathRemover[F],
          EventLogTableRenamer[F],
          EventStatusRenamer[F],
          EventPayloadTableCreator[F],
          SubscriptionCategorySyncTimeTableCreator[F],
          StatusesProcessingTimeTableCreator[F],
          SubscriberTableCreator[F],
          EventDeliveryTableCreator[F],
          TimestampZoneAdder[F],
          PayloadTypeChanger[F],
          StatusChangeEventsTableCreator[F],
          EventDeliveryEventTypeAdder[F],
          EventDeliveryEventTypeAdder[F],
          TSMigrationTableCreator[F],
          CleanUpEventsTableCreator[F],
          FailedEventsRestorer[F](
            "%Error: The repository is dirty. Please use the \"git\" command to clean it.%",
            currentStatus = GenerationNonRecoverableFailure,
            destinationStatus = New,
            discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%BadRequestException: POST http://renku-jena-master:3030/renku/update returned 400 Bad Request; body: Error 400: Lexical error%",
            currentStatus = TransformationNonRecoverableFailure,
            destinationStatus = TriplesGenerated,
            discardingStatuses = TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%remote: HTTP Basic: Access denied%",
            currentStatus = GenerationNonRecoverableFailure,
            destinationStatus = New,
            discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%fatal: could not read Username for%",
            currentStatus = GenerationNonRecoverableFailure,
            destinationStatus = New,
            discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%Error: Cannot find object:%",
            currentStatus = GenerationNonRecoverableFailure,
            destinationStatus = New,
            discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%Not equal entity(ies) in json-ld%",
            currentStatus = TransformationNonRecoverableFailure,
            destinationStatus = TriplesGenerated,
            discardingStatuses = TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%fatal: not removing ''.'' recursively without -r%",
            currentStatus = GenerationNonRecoverableFailure,
            destinationStatus = New,
            discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
          ),
          FailedEventsRestorer[F](
            "%Cannot decode % to Instant: Text % could not be parsed at index%",
            currentStatus = TransformationNonRecoverableFailure,
            destinationStatus = New,
            discardingStatuses = TriplesGenerated :: TriplesStore :: Nil
          )
        ),
        isMigrating
      )
    }
}
