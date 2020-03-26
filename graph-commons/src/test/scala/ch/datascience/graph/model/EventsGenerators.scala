/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events._
import org.scalacheck.Gen
import org.scalacheck.Gen._

object EventsGenerators {

  implicit val commitIds:      Gen[CommitId]      = shas map CommitId.apply
  implicit val commitMessages: Gen[CommitMessage] = nonEmptyStrings() map CommitMessage.apply
  implicit val committedDates: Gen[CommittedDate] = timestampsNotInTheFuture map CommittedDate.apply
  implicit val batchDates:     Gen[BatchDate]     = timestampsNotInTheFuture map BatchDate.apply

  implicit val authors: Gen[Author] = Gen.oneOf(
    usernames map Author.withUsername,
    userEmails map Author.withEmail,
    for {
      username <- usernames
      email    <- userEmails
    } yield Author(username, email)
  )

  implicit val committers: Gen[Committer] = Gen.oneOf(
    usernames map Committer.withUsername,
    userEmails map Committer.withEmail,
    for {
      username <- usernames
      email    <- userEmails
    } yield Committer(username, email)
  )

  implicit val projects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPaths
  } yield Project(projectId, path)

  implicit def parentsIdsLists(minNumber: Int = 0, maxNumber: Int = 4): Gen[List[CommitId]] = {
    require(minNumber <= maxNumber,
            s"minNumber = $minNumber is not <= maxNumber = $maxNumber for generating parents Ids list")

    for {
      parentCommitsNumber <- choose(minNumber, maxNumber)
      parents             <- Gen.listOfN(parentCommitsNumber, commitIds)
    } yield parents
  }

  implicit val commitEventIds: Gen[CommitEventId] = for {
    eventId   <- commitIds
    projectId <- projectIds
  } yield CommitEventId(eventId, projectId)

  implicit val commitEvents: Gen[CommitEvent] = for {
    commitId      <- commitIds
    project       <- projects
    message       <- commitMessages
    committedDate <- committedDates
    author        <- authors
    committer     <- committers
    parentsIds    <- parentsIdsLists()
    batchDate     <- batchDates
  } yield CommitEvent(commitId, project, message, committedDate, author, committer, parentsIds, batchDate)
}
