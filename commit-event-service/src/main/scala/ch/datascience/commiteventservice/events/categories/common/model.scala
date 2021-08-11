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

package ch.datascience.commiteventservice.events.categories.common

import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.EventStatus.{New, Skipped}
import ch.datascience.graph.model.events.{BatchDate, CommitId, CommitMessage, CommittedDate, CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.{projects, users}
import ch.datascience.graph.model.projects.{Id, Path, Visibility}

private[categories] final case class ProjectInfo(
    id:         Id,
    visibility: Visibility,
    path:       Path
)

private[categories] final case class Commit(id: CommitId, project: Project)

private[categories] final case class CommitWithParents(id: CommitId, projectId: projects.Id, parents: List[CommitId])

private[categories] sealed trait CommitEvent extends Product with Serializable {
  def id:            CommitId
  def project:       Project
  def message:       CommitMessage
  def committedDate: CommittedDate
  def author:        Author
  def committer:     Committer
  def parents:       List[CommitId]
  def batchDate:     BatchDate
  def status:        EventStatus
}

private[categories] object CommitEvent {
  implicit class CommitEventOps(commitEvent: CommitEvent) {
    lazy val compoundEventId: CompoundEventId = CompoundEventId(EventId(commitEvent.id.value), commitEvent.project.id)
  }

  final case class NewCommitEvent(
      id:            CommitId,
      project:       Project,
      message:       CommitMessage,
      committedDate: CommittedDate,
      author:        Author,
      committer:     Committer,
      parents:       List[CommitId],
      batchDate:     BatchDate
  ) extends CommitEvent {
    override def status: EventStatus = New
  }

  final case class SkippedCommitEvent(
      id:            CommitId,
      project:       Project,
      message:       CommitMessage,
      committedDate: CommittedDate,
      author:        Author,
      committer:     Committer,
      parents:       List[CommitId],
      batchDate:     BatchDate
  ) extends CommitEvent {
    override def status: EventStatus = Skipped
  }
}

private[categories] sealed trait Person extends Product with Serializable {
  def name: users.Name
}

private[categories] object Person {
  sealed trait WithEmail { self: Person =>
    def email: Email
  }

  import io.circe.Json
  import io.circe.syntax._

  implicit class PersonOps(person: Person) {

    lazy val emailToJson: Json = person match {
      case person: Person.WithEmail => person.email.value.asJson
      case _ => Json.Null
    }
  }
}

import ch.datascience.commiteventservice.events.categories.common.Person._

private[categories] sealed trait Author extends Person
private[categories] object Author {
  final case class FullAuthor(name: users.Name, email: Email) extends Author with WithEmail
  final case class AuthorWithName(name: users.Name) extends Author

  def apply(username:    users.Name, email: Email): Author = FullAuthor(username, email)
  def withName(username: users.Name): Author = AuthorWithName(username)
  def withEmail(email:   Email): Author = FullAuthor(email.extractName, email)
}

private[categories] sealed trait Committer extends Person
private[categories] object Committer {
  final case class FullCommitter(name: users.Name, email: Email) extends Committer with WithEmail
  final case class CommitterWithName(name: users.Name) extends Committer

  def apply(username:    users.Name, email: Email): Committer = FullCommitter(username, email)
  def withName(username: users.Name): Committer = CommitterWithName(username)
  def withEmail(email:   Email): Committer = FullCommitter(email.extractName, email)
}
