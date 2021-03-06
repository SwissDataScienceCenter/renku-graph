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

package ch.datascience.commiteventservice.events.categories.commitsync

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.EventsGenerators.{commitIds, lastSyncedDates}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalacheck.Gen

private object Generators {

  lazy val fullCommitSyncEvents: Gen[FullCommitSyncEvent] = for {
    commitId       <- commitIds
    projectId      <- projectIds
    projectPath    <- projectPaths
    lastSyncedDate <- lastSyncedDates
  } yield FullCommitSyncEvent(commitId, Project(projectId, projectPath), lastSyncedDate)

  lazy val minimalCommitSyncEvents: Gen[MinimalCommitSyncEvent] = for {
    projectId   <- projectIds
    projectPath <- projectPaths
  } yield MinimalCommitSyncEvent(Project(projectId, projectPath))

  lazy val commitSyncEvents: Gen[CommitSyncEvent] = Gen.oneOf(fullCommitSyncEvents, minimalCommitSyncEvents)
}
