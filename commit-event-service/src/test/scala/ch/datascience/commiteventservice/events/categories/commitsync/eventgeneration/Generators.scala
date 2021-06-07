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

package ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration

import ch.datascience.commiteventservice.events.categories.commitsync.ProjectInfo
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import ch.datascience.events.consumers.Project
import ch.datascience.generators.Generators.listOf
import ch.datascience.graph.model.EventsGenerators.{batchDates, commitIds, commitMessages, committedDates}
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths, projectVisibilities, userEmails, userNames}
import ch.datascience.graph.model.events.CommitId
import org.scalacheck.Gen
import org.scalacheck.Gen.choose

private object Generators {

  implicit val commits: Gen[Commit] = for {
    id      <- commitIds
    project <- projects
  } yield Commit(id, project)

  implicit val projectInfos: Gen[ProjectInfo] = for {
    id         <- projectIds
    visibility <- projectVisibilities
    path       <- projectPaths
  } yield ProjectInfo(id, visibility, path)

  implicit val commitInfos: Gen[CommitInfo] = for {
    id            <- commitIds
    message       <- commitMessages
    committedDate <- committedDates
    author        <- authors
    committer     <- committers
    parents       <- listOf(commitIds)
  } yield CommitInfo(id, message, committedDate, author, committer, parents)

  implicit lazy val newCommitEvents: Gen[CommitEvent] = for {
    commitId      <- commitIds
    project       <- projects
    message       <- commitMessages
    committedDate <- committedDates
    author        <- authors
    committer     <- committers
    parentsIds    <- parentsIdsLists()
    batchDate     <- batchDates
  } yield NewCommitEvent(commitId, project, message, committedDate, author, committer, parentsIds, batchDate)

  implicit lazy val skippedCommitEvents: Gen[SkippedCommitEvent] = for {
    commitId      <- commitIds
    project       <- projects
    message       <- commitMessages
    committedDate <- committedDates
    author        <- authors
    committer     <- committers
    parentsIds    <- parentsIdsLists()
    batchDate     <- batchDates
  } yield SkippedCommitEvent(commitId, project, message, committedDate, author, committer, parentsIds, batchDate)

  implicit lazy val authors: Gen[Author] = Gen.oneOf(
    userNames map Author.withName,
    userEmails map Author.withEmail,
    for {
      username <- userNames
      email    <- userEmails
    } yield Author(username, email)
  )

  implicit lazy val committers: Gen[Committer] = Gen.oneOf(
    userNames map Committer.withName,
    userEmails map Committer.withEmail,
    for {
      username <- userNames
      email    <- userEmails
    } yield Committer(username, email)
  )

  implicit lazy val projects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPaths
  } yield Project(projectId, path)

  implicit def parentsIdsLists(minNumber: Int = 0, maxNumber: Int = 4): Gen[List[CommitId]] = {
    require(minNumber <= maxNumber,
            s"minNumber = $minNumber is not <= maxNumber = $maxNumber for generating parents Ids list"
    )

    for {
      parentCommitsNumber <- choose(minNumber, maxNumber)
      parents             <- Gen.listOfN(parentCommitsNumber, commitIds)
    } yield parents
  }
}
