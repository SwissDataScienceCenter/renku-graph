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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.{AllEventsToNew, _}
import io.renku.graph.model.events.EventStatus.{FailureStatus, ProcessingStatus}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.Session

private trait DBUpdater[F[_], E <: StatusChangeEvent] {
  def updateDB(event:   E): UpdateResult[F]
  def onRollback(event: E): Kleisli[F, Session[F], Unit]
}

private object DBUpdater {

  type EventUpdaterFactory[F[_], E <: StatusChangeEvent] =
    (StatusChangeEventsQueue[F], DeliveryInfoRemover[F], LabeledHistogram[F, SqlStatement.Name]) => F[DBUpdater[F, E]]

  implicit def factoryToTriplesGeneratorUpdater[F[_]: Async]: EventUpdaterFactory[F, ToTriplesGenerated] =
    (_, infoRemover, execTimes) =>
      new ToTriplesGeneratedUpdater(infoRemover, execTimes).pure[F].widen[DBUpdater[F, ToTriplesGenerated]]

  implicit def factoryToTriplesStoreUpdater[F[_]: Async]: EventUpdaterFactory[F, ToTriplesStore] =
    (_, infoRemover, execTimes) =>
      new ToTriplesStoreUpdater(infoRemover, execTimes).pure[F].widen[DBUpdater[F, ToTriplesStore]]

  implicit def factoryToFailureUpdater[F[_]: Async]
      : EventUpdaterFactory[F, ToFailure[ProcessingStatus, FailureStatus]] = (_, infoRemover, execTimes) =>
    new ToFailureUpdater(infoRemover, execTimes).pure[F].widen[DBUpdater[F, ToFailure[ProcessingStatus, FailureStatus]]]

  implicit def factoryRollbackToNewUpdater[F[_]: MonadCancelThrow]: EventUpdaterFactory[F, RollbackToNew] =
    (_, _, execTimes) => new RollbackToNewUpdater(execTimes).pure[F].widen[DBUpdater[F, RollbackToNew]]

  implicit def factoryRollbackToTriplesGeneratedUpdater[F[_]: MonadCancelThrow]
      : EventUpdaterFactory[F, RollbackToTriplesGenerated] = (_, _, execTimes) =>
    new RollbackToTriplesGeneratedUpdater(execTimes).pure[F].widen[DBUpdater[F, RollbackToTriplesGenerated]]

  implicit def factoryToAwaitingDeletionUpdater[F[_]: MonadCancelThrow]: EventUpdaterFactory[F, ToAwaitingDeletion] =
    (_, _, execTimes) => new ToAwaitingDeletionUpdater(execTimes).pure[F].widen[DBUpdater[F, ToAwaitingDeletion]]

  implicit def factoryRollbackToAwaitingDeletionUpdater[F[_]: MonadCancelThrow]
      : EventUpdaterFactory[F, RollbackToAwaitingDeletion] =
    (_, _, execTimes) =>
      new RollbackToAwaitingDeletionUpdater(execTimes).pure[F].widen[DBUpdater[F, RollbackToAwaitingDeletion]]

  implicit def factoryToProjectEventsNewHandler[F[_]: Async: Logger]: EventUpdaterFactory[F, ProjectEventsToNew] =
    (eventsQueue, _, _) => new ProjectEventsToNewHandler[F](eventsQueue).pure[F].widen[DBUpdater[F, ProjectEventsToNew]]

  implicit def factoryToAllEventsNewUpdater[F[_]: Async: Logger]: EventUpdaterFactory[F, AllEventsToNew] =
    (_, _, execTimes) => AllEventsToNewUpdater(execTimes).widen[DBUpdater[F, AllEventsToNew]]
}
