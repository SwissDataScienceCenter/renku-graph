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

import cats.effect.{BracketThrow, Sync}
import ch.datascience.db.SqlStatement
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent._

private trait DBUpdater[Interpretation[_], E <: StatusChangeEvent] {
  def updateDB(event: E): UpdateResult[Interpretation]
}

private object DBUpdater {
  def apply[Interpretation[_]: BracketThrow: Sync, E <: StatusChangeEvent]()(implicit
      queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
      factory:          LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, E]
  ) = factory(queriesExecTimes)

  implicit def factoryToTriplesGeneratorUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, ToTriplesGenerated] =
    new ToTriplesGeneratedUpdater(_)

  implicit def factoryToTriplesStoreUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, ToTriplesStore] =
    new ToTriplesStoreUpdater(_)

  implicit def factoryToGenerationRecoverableFailureUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation,
                                                                         ToGenerationRecoverableFailure
      ] = new ToGenerationRecoverableFailureUpdater(_)

  implicit def factoryRollbackToNewUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, RollbackToNew] =
    new RollbackToNewUpdater(_)

  implicit def factoryRollbackToTriplesGeneratedUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, RollbackToTriplesGenerated] =
    new RollbackToTriplesGeneratedUpdater(_)

  implicit def factoryToAwaitingDeletionUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, ToAwaitingDeletion] =
    new ToAwaitingDeletionUpdater(_)

  implicit def factoryToAllEventsNewUpdater[Interpretation[_]: BracketThrow: Sync]
      : LabeledHistogram[Interpretation, SqlStatement.Name] => DBUpdater[Interpretation, AllEventsToNew] =
    new AllEventsToNewUpdater[Interpretation](_)
}
