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

object EventsGenerators {

  implicit val commitIds: Gen[CommitId] = shas map CommitId.apply
  implicit val userIds:   Gen[UserId]   = nonNegativeInts() map UserId.apply
  implicit val usernames: Gen[Username] = nonEmptyStrings() map Username.apply
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

  implicit val projectIds:  Gen[ProjectId]   = nonNegativeInts() map ProjectId.apply
  implicit val projectPath: Gen[ProjectPath] = relativePaths map ProjectPath.apply

  implicit val projects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPath
  } yield Project(projectId, path)

  implicit val commitEvents: Gen[CommitEvent] = for {
    commitId            <- commitIds
    message             <- nonEmptyStrings()
    timestamp           <- timestampsInThePast
    pushUser            <- pushUsers
    author              <- users
    committer           <- users
    parentCommitsNumber <- nonNegativeInts(4)
    parents             <- Gen.listOfN(parentCommitsNumber, commitIds)
    project             <- projects
  } yield CommitEvent(commitId, message, timestamp, pushUser, author, committer, parents, project, Nil, Nil, Nil)
}
