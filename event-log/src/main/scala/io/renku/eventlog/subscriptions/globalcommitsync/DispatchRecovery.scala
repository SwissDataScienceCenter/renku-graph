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

package io.renku.eventlog.subscriptions.globalcommitsync

import cats.effect.{BracketThrow, IO, Timer}
import cats.syntax.all._
import io.renku.eventlog.subscriptions
import io.renku.eventlog.subscriptions.DispatchRecovery
import io.renku.events.consumers.subscriptions.SubscriberUrl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private class DispatchRecoveryImpl[Interpretation[_]: BracketThrow](
    lastSyncUpdater: LastSyncedDateUpdater[Interpretation],
    logger:          Logger[Interpretation]
) extends subscriptions.DispatchRecovery[Interpretation, GlobalCommitSyncEvent] {

  override def returnToQueue(event: GlobalCommitSyncEvent): Interpretation[Unit] =
    (lastSyncUpdater run (event.project.id, event.maybeLastSyncedDate)).void

  override def recover(
      url:   SubscriberUrl,
      event: GlobalCommitSyncEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    for {
      _ <- lastSyncUpdater run (event.project.id, event.maybeLastSyncedDate)
      _ <- logger.error(exception)(
             s"$categoryName: $event, url = $url -> ${event.project.show} ${event.maybeLastSyncedDate}"
           )
    } yield ()
  }

}

private object DispatchRecovery {

  def apply(
      lastSyncedDateUpdater: LastSyncedDateUpdater[IO],
      logger:                Logger[IO]
  )(implicit timer:          Timer[IO]): IO[DispatchRecovery[IO, GlobalCommitSyncEvent]] = IO {
    new DispatchRecoveryImpl[IO](lastSyncedDateUpdater, logger)
  }
}
