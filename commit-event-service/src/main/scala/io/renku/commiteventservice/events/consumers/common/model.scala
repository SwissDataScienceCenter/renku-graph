/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus.{New, Skipped}
import io.renku.graph.model.events.{BatchDate, CommitId, CommitMessage, CommittedDate, CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.persons.Email
import io.renku.graph.model.projects.{GitLabId, Slug, Visibility}
import io.renku.graph.model.{persons, projects}

private[consumers] final case class ProjectInfo(
    id:         GitLabId,
    visibility: Visibility,
    slug:       Slug
)

private[consumers] final case class Commit(id: CommitId, project: Project)

private[consumers] final case class CommitWithParents(id:        CommitId,
                                                      projectId: projects.GitLabId,
                                                      parents:   List[CommitId]
)

private[consumers] sealed trait CommitEvent extends Product with Serializable {
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

private[consumers] object CommitEvent {
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

private[consumers] sealed trait Person extends Product with Serializable {
  def name: persons.Name
}

private[consumers] object Person {
  sealed trait WithEmail extends Person {
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

import io.renku.commiteventservice.events.consumers.common.Person._

private[consumers] sealed trait Author extends Person
private[consumers] object Author {
  final case class FullAuthor(name: persons.Name, email: Email) extends Author with WithEmail
  final case class AuthorWithName(name: persons.Name)           extends Author

  def apply(username:    persons.Name, email: Email): Author = FullAuthor(username, email)
  def withName(username: persons.Name): Author = AuthorWithName(username)
  def withEmail(email:   Email): Author = FullAuthor(email.extractName, email)
}

private[consumers] sealed trait Committer extends Person
private[consumers] object Committer {
  final case class FullCommitter(name: persons.Name, email: Email) extends Committer with WithEmail
  final case class CommitterWithName(name: persons.Name)           extends Committer

  def apply(username:    persons.Name, email: Email): Committer = FullCommitter(username, email)
  def withName(username: persons.Name): Committer = CommitterWithName(username)
  def withEmail(email:   Email): Committer = FullCommitter(email.extractName, email)
}
