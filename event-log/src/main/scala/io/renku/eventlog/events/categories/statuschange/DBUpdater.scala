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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.effect.kernel.Async
import io.renku.db.SqlStatement
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.{AllEventsToNew, _}
import io.renku.graph.model.events.EventStatus.{FailureStatus, ProcessingStatus}
import io.renku.metrics.LabeledHistogram
import skunk.Session

private trait DBUpdater[F[_], E <: StatusChangeEvent] {
  def updateDB(event:   E): UpdateResult[F]
  def onRollback(event: E): Kleisli[F, Session[F], Unit]
}

private object DBUpdater {

  type EventUpdaterFactory[F[_], E <: StatusChangeEvent] =
    (DeliveryInfoRemover[F], LabeledHistogram[F, SqlStatement.Name]) => DBUpdater[F, E]

  implicit def factoryToTriplesGeneratorUpdater[F[_]: Async]: EventUpdaterFactory[F, ToTriplesGenerated] =
    new ToTriplesGeneratedUpdater(_, _)

  implicit def factoryToTriplesStoreUpdater[F[_]: Async]: EventUpdaterFactory[F, ToTriplesStore] =
    new ToTriplesStoreUpdater(_, _)

  implicit def factoryToFailureUpdater[F[_]: Async]
      : EventUpdaterFactory[F, ToFailure[ProcessingStatus, FailureStatus]] = new ToFailureUpdater(_, _)

  implicit def factoryRollbackToNewUpdater[F[_]: MonadCancelThrow]: EventUpdaterFactory[F, RollbackToNew] =
    (_, execTimes) => new RollbackToNewUpdater(execTimes)

  implicit def factoryRollbackToTriplesGeneratedUpdater[F[_]: MonadCancelThrow]
      : EventUpdaterFactory[F, RollbackToTriplesGenerated] = (_, execTimes) =>
    new RollbackToTriplesGeneratedUpdater(execTimes)

  implicit def factoryToAwaitingDeletionUpdater[F[_]: MonadCancelThrow]: EventUpdaterFactory[F, ToAwaitingDeletion] =
    (_, execTimes) => new ToAwaitingDeletionUpdater(execTimes)

  implicit def factoryToAllEventsNewUpdater[F[_]: MonadCancelThrow]: EventUpdaterFactory[F, AllEventsToNew] =
    (_, execTimes) => new AllEventsToNewUpdater[F](execTimes)

  implicit def factoryToProjectEventsNewUpdater[F[_]: Async]: EventUpdaterFactory[F, ProjectEventsToNew] =
    (_, execTimes) => new ProjectEventsToNewUpdater[F](execTimes)
}
