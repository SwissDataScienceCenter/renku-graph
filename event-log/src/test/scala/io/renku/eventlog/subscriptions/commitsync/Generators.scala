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

package io.renku.eventlog.subscriptions.commitsync

import ch.datascience.events.consumers.ConsumersModelGenerators.projectsGen
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.timestampsNotInTheFuture
import ch.datascience.graph.model.EventsGenerators.compoundEventIds
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.LastSyncedDate
import org.scalacheck.Gen

private object Generators {

  val fullCommitSyncEvents: Gen[FullCommitSyncEvent] = for {
    id         <- compoundEventIds
    path       <- projectPaths
    lastSynced <- timestampsNotInTheFuture toGeneratorOf LastSyncedDate
  } yield FullCommitSyncEvent(id, path, lastSynced)

  val minimalCommitSyncEvents: Gen[MinimalCommitSyncEvent] = for {
    project <- projectsGen
  } yield MinimalCommitSyncEvent(project)

  val commitSyncEvents: Gen[CommitSyncEvent] = Gen.oneOf(fullCommitSyncEvents, minimalCommitSyncEvents)
}
