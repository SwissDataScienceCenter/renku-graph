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

import java.time.Instant

import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.model.users.{Email, Username}
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

object events {

  final case class CommitEvent(
      id:            CommitId,
      project:       Project,
      message:       CommitMessage,
      committedDate: CommittedDate,
      author:        User,
      committer:     User,
      parents:       List[CommitId]
  )

  object CommitEvent {

    implicit class CommitEventOps(commitEvent: CommitEvent) {
      lazy val commitEventId: CommitEventId = CommitEventId(commitEvent.id, commitEvent.project.id)
    }
  }

  final case class Project(
      id:   ProjectId,
      path: ProjectPath
  )

  final case class User(
      username: Username,
      email:    Email
  )

  final case class CommitEventId(id: CommitId, projectId: ProjectId) {
    override lazy val toString: String = s"id = $id, projectId = $projectId"
  }

  final class CommitId private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitId extends TinyTypeFactory[CommitId](new CommitId(_)) with GitSha

  final class CommitMessage private (val value: String) extends AnyVal with StringTinyType
  implicit object CommitMessage extends TinyTypeFactory[CommitMessage](new CommitMessage(_)) with NonBlank

  final class CommittedDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object CommittedDate extends TinyTypeFactory[CommittedDate](new CommittedDate(_)) with InstantNotInTheFuture

  final class ProjectId private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProjectId extends TinyTypeFactory[ProjectId](new ProjectId(_)) with NonNegativeInt
}
