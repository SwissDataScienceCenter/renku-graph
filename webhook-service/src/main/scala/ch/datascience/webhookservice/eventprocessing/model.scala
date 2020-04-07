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

package ch.datascience.webhookservice.eventprocessing

import ch.datascience.graph.model.events.{BatchDate, CommitId, CommitMessage, CommittedDate, CompoundEventId, EventId}
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.{projects, users}

final case class StartCommit(
    id:      CommitId,
    project: Project
)

final case class CommitEvent(
    id:            CommitId,
    project:       Project,
    message:       CommitMessage,
    committedDate: CommittedDate,
    author:        Author,
    committer:     Committer,
    parents:       List[CommitId],
    batchDate:     BatchDate
)

object CommitEvent {
  implicit class CommitEventOps(commitEvent: CommitEvent) {
    lazy val compoundEventId: CompoundEventId = CompoundEventId(EventId(commitEvent.id.value), commitEvent.project.id)
  }
}

final case class Project(id: projects.Id, path: projects.Path)

sealed trait Person extends Product with Serializable {
  def name: users.Name
}

object Person {
  sealed trait WithEmail { self: Person =>
    def email: Email
  }
}

import Person._

sealed trait Author extends Person
object Author {
  final case class FullAuthor(name:     users.Name, email: Email) extends Author with WithEmail
  final case class AuthorWithName(name: users.Name) extends Author

  def apply(username:    users.Name, email: Email): Author = FullAuthor(username, email)
  def withName(username: users.Name): Author = AuthorWithName(username)
  def withEmail(email:   Email): Author = FullAuthor(email.extractName, email)
}

sealed trait Committer extends Person
object Committer {
  final case class FullCommitter(name:     users.Name, email: Email) extends Committer with WithEmail
  final case class CommitterWithName(name: users.Name) extends Committer

  def apply(username:    users.Name, email: Email): Committer = FullCommitter(username, email)
  def withName(username: users.Name): Committer = CommitterWithName(username)
  def withEmail(email:   Email): Committer = FullCommitter(email.extractName, email)
}
