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

import java.time.{Clock, Instant}

import ch.datascience.graph.model.users.{Email, Username}
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

object events {

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
      lazy val commitEventId: CommitEventId = CommitEventId(commitEvent.id, commitEvent.project.id)
    }
  }

  final case class Project(id: projects.Id, path: projects.Path)

  sealed trait Person extends Product with Serializable {
    def username: Username
  }

  object Person {
    sealed trait WithEmail { self: Person =>
      def email: Email
    }
  }

  import Person._

  sealed trait Author extends Person
  object Author {
    final case class FullAuthor(username:         Username, email: Email) extends Author with WithEmail
    final case class AuthorWithUsername(username: Username) extends Author

    def apply(username:        Username, email: Email): Author = FullAuthor(username, email)
    def withUsername(username: Username): Author = AuthorWithUsername(username)
    def withEmail(email:       Email): Author = FullAuthor(email.extractUsername, email)
  }

  sealed trait Committer extends Person
  object Committer {
    final case class FullCommitter(username:         Username, email: Email) extends Committer with WithEmail
    final case class CommitterWithUsername(username: Username) extends Committer

    def apply(username:        Username, email: Email): Committer = FullCommitter(username, email)
    def withUsername(username: Username): Committer = CommitterWithUsername(username)
    def withEmail(email:       Email): Committer = FullCommitter(email.extractUsername, email)
  }

  final case class CommitEventId(id: CommitId, projectId: projects.Id) {
    override lazy val toString: String = s"id = $id, projectId = $projectId"
  }

  final class CommitId private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitId extends TinyTypeFactory[CommitId](new CommitId(_)) with GitSha

  final class CommitMessage private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitMessage extends TinyTypeFactory[CommitMessage](new CommitMessage(_)) with NonBlank

  final class CommittedDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object CommittedDate extends TinyTypeFactory[CommittedDate](new CommittedDate(_)) with InstantNotInTheFuture

  final class BatchDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object BatchDate extends TinyTypeFactory[BatchDate](new BatchDate(_)) with InstantNotInTheFuture {
    def apply(clock: Clock): BatchDate = apply(clock.instant())
  }
}
