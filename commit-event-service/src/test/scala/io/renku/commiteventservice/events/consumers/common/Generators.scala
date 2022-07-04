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

package io.renku.commiteventservice.events.consumers.common

import io.renku.commiteventservice.events.consumers.common
import io.renku.commiteventservice.events.consumers.common.CommitEvent.{NewCommitEvent, SkippedCommitEvent}
import io.renku.events.consumers.ConsumersModelGenerators._
import io.renku.generators.Generators.listOf
import io.renku.graph.model.EventsGenerators.{batchDates, commitIds, commitMessages, committedDates}
import io.renku.graph.model.GraphModelGenerators.{personEmails, personNames, projectIds, projectPaths, projectVisibilities}
import io.renku.graph.model.events.CommitId
import org.scalacheck.Gen
import org.scalacheck.Gen.choose

private[consumers] object Generators {

  implicit val commits: Gen[Commit] = for {
    id      <- commitIds
    project <- consumerProjects
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
  } yield common.CommitInfo(id, message, committedDate, author, committer, parents)

  implicit lazy val newCommitEvents: Gen[CommitEvent] = for {
    commitId      <- commitIds
    project       <- consumerProjects
    message       <- commitMessages
    committedDate <- committedDates
    author        <- authors
    committer     <- committers
    parentsIds    <- parentsIdsLists()
    batchDate     <- batchDates
  } yield NewCommitEvent(commitId, project, message, committedDate, author, committer, parentsIds, batchDate)

  implicit lazy val skippedCommitEvents: Gen[SkippedCommitEvent] = for {
    commitId      <- commitIds
    project       <- consumerProjects
    message       <- commitMessages
    committedDate <- committedDates
    author        <- authors
    committer     <- committers
    parentsIds    <- parentsIdsLists()
    batchDate     <- batchDates
  } yield SkippedCommitEvent(commitId, project, message, committedDate, author, committer, parentsIds, batchDate)

  implicit lazy val authors: Gen[Author] = Gen.oneOf(
    personNames map Author.withName,
    personEmails map Author.withEmail,
    for {
      username <- personNames
      email    <- personEmails
    } yield Author(username, email)
  )

  implicit lazy val committers: Gen[Committer] = Gen.oneOf(
    personNames map Committer.withName,
    personEmails map Committer.withEmail,
    for {
      username <- personNames
      email    <- personEmails
    } yield Committer(username, email)
  )

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
