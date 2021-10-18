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

package io.renku.commiteventservice.events.categories.globalcommitsync

import eu.timepit.refined.api.Refined
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import org.scalacheck.Gen

private object Generators {

  lazy val globalCommitSyncEventsNonZero: Gen[GlobalCommitSyncEvent] = globalCommitSyncEvents(
    CommitCount(positiveInts(max = 9999).generateOne.value)
  )

  def globalCommitSyncEvents(commitCount: CommitCount): Gen[GlobalCommitSyncEvent] =
    globalCommitSyncEvents(commitIdsGen =
      listOf(commitIds,
             minElements = Refined.unsafeApply(commitCount.value),
             maxElements = Refined.unsafeApply(commitCount.value)
      )
    )

  lazy val commitCounts: Gen[CommitCount] = positiveInts().map(_.value).toGeneratorOf(CommitCount)

  def globalCommitSyncEvents(projectIdGen: Gen[projects.Id] = projectIds,
                             commitIdsGen: Gen[List[CommitId]] = listOf(commitIds)
  ): Gen[GlobalCommitSyncEvent] = for {
    projectId   <- projectIdGen
    projectPath <- projectPaths
    commitIds   <- commitIdsGen
  } yield GlobalCommitSyncEvent(Project(projectId, projectPath), commitIds)

  def projectCommitStats(commitId:    CommitId): Gen[ProjectCommitStats] = projectCommitStats(Gen.const(Some(commitId)))
  def projectCommitStats(commitIdGen: Gen[Option[CommitId]] = commitIds.toGeneratorOfOptions): Gen[ProjectCommitStats] =
    for {
      maybeCommitId <- commitIdGen
      commitCount   <- nonNegativeInts(9999999)
    } yield ProjectCommitStats(maybeCommitId, CommitCount(commitCount.value))
}
