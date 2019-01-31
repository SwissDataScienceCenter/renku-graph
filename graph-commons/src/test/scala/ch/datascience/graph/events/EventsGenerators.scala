/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.events

import ch.datascience.generators.Generators._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import GraphCommonsGenerators._

object EventsGenerators {

  implicit val commitIds:      Gen[CommitId]      = shas map CommitId.apply
  implicit val commitMessages: Gen[CommitMessage] = nonEmptyStrings() map CommitMessage.apply
  implicit val committedDates: Gen[CommittedDate] = timestampsInThePast map CommittedDate.apply
  implicit val userIds:        Gen[UserId]        = nonNegativeInts() map UserId.apply
  implicit val usernames:      Gen[Username]      = nonEmptyStrings() map Username.apply
  implicit val emails: Gen[Email] = for {
    beforeAt <- nonEmptyStrings()
    afterAt  <- nonEmptyStrings()
  } yield Email(s"$beforeAt@$afterAt")

  implicit val pushUsers: Gen[PushUser] = for {
    userId   <- userIds
    username <- usernames
    email    <- emails
  } yield PushUser(userId, username, email)

  implicit val users: Gen[User] = for {
    username <- usernames
    email    <- emails
  } yield User(username, email)

  implicit val projectIds:   Gen[ProjectId]   = nonNegativeInts() map ProjectId.apply
  implicit val projectPaths: Gen[ProjectPath] = relativePaths(minSegments = 2, maxSegments = 2) map ProjectPath.apply

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

  implicit val commitEvents: Gen[CommitEvent] = for {
    commitId        <- commitIds
    message         <- commitMessages
    committedDate   <- committedDates
    pushUser        <- pushUsers
    author          <- users
    committer       <- users
    parentsIds      <- parentsIdsLists()
    project         <- projects
    hookAccessToken <- hookAccessTokens
  } yield
    CommitEvent(commitId, message, committedDate, pushUser, author, committer, parentsIds, project, hookAccessToken)
}
