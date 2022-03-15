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

package io.renku.commiteventservice.events.categories.globalcommitsync

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.auto._
import io.renku.commiteventservice.events.categories.globalcommitsync.GlobalCommitSyncEvent.CommitsInfo
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.{DateCondition, PageResult, ProjectCommitStats}
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.pages
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import org.scalacheck.Gen

private object Generators {

  lazy val commitsCounts: Gen[CommitsCount] = positiveLongs().map(_.value).toGeneratorOf(CommitsCount)

  def globalCommitSyncEvents(projectIdGen: Gen[projects.Id] = projectIds): Gen[GlobalCommitSyncEvent] = for {
    projectId   <- projectIdGen
    projectPath <- projectPaths
    commitsInfo <- commitsInfos
  } yield GlobalCommitSyncEvent(Project(projectId, projectPath), commitsInfo)

  lazy val commitsInfos: Gen[CommitsInfo] = for {
    commitsCount   <- commitsCounts
    latestCommitId <- commitIds
  } yield CommitsInfo(commitsCount, latestCommitId)

  def pageResults(maxCommitCount: Int Refined Positive = positiveInts().generateOne) = for {
    commits       <- commitIds.toGeneratorOfList(0, maxCommitCount)
    maybeNextPage <- pages.toGeneratorOfOptions
  } yield PageResult(commits, maybeNextPage)

  def projectCommitStats(commitId: CommitId): Gen[ProjectCommitStats] = projectCommitStats(Gen.const(Some(commitId)))

  def projectCommitStats(commitIdGen: Gen[Option[CommitId]] = commitIds.toGeneratorOfOptions): Gen[ProjectCommitStats] =
    for {
      maybeCommitId <- commitIdGen
      commitCount   <- nonNegativeInts(9999999)
    } yield ProjectCommitStats(maybeCommitId, CommitsCount(commitCount.value))

  implicit val untilDateConditions: Gen[DateCondition.Until] = timestamps.map(DateCondition.Until).generateOne
  implicit val sinceDateConditions: Gen[DateCondition.Since] = timestamps.map(DateCondition.Since).generateOne
  implicit val dateConditions:      Gen[DateCondition]       = Gen.oneOf(untilDateConditions, sinceDateConditions)
}
