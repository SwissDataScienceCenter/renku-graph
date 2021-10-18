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
import cats.effect.{BracketThrow, Sync}
import io.renku.db.SqlStatement
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.{AllEventsToNew, _}
import io.renku.graph.model.events.EventStatus.{FailureStatus, ProcessingStatus}
import io.renku.metrics.LabeledHistogram
import skunk.Session

private trait DBUpdater[Interpretation[_], E <: StatusChangeEvent] {
  def updateDB(event:   E): UpdateResult[Interpretation]
  def onRollback(event: E): Kleisli[Interpretation, Session[Interpretation], Unit]
}

private object DBUpdater {

  type EventUpdaterFactory[Interpretation[_], E <: StatusChangeEvent] =
    (DeliveryInfoRemover[Interpretation],
     LabeledHistogram[Interpretation, SqlStatement.Name]
    ) => DBUpdater[Interpretation, E]

  implicit def factoryToTriplesGeneratorUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, ToTriplesGenerated] = new ToTriplesGeneratedUpdater(_, _)

  implicit def factoryToTriplesStoreUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, ToTriplesStore] = new ToTriplesStoreUpdater(_, _)

  implicit def factoryToFailureUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, ToFailure[ProcessingStatus, FailureStatus]] = new ToFailureUpdater(_, _)

  implicit def factoryRollbackToNewUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, RollbackToNew] = (_, execTimes) => new RollbackToNewUpdater(execTimes)

  implicit def factoryRollbackToTriplesGeneratedUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, RollbackToTriplesGenerated] = (_, execTimes) =>
    new RollbackToTriplesGeneratedUpdater(execTimes)

  implicit def factoryToAwaitingDeletionUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, ToAwaitingDeletion] = (_, execTimes) =>
    new ToAwaitingDeletionUpdater(execTimes)

  implicit def factoryToAllEventsNewUpdater[Interpretation[_]: BracketThrow: Sync]
      : EventUpdaterFactory[Interpretation, AllEventsToNew] = (_, execTimes) =>
    new AllEventsToNewUpdater[Interpretation](execTimes)
}
