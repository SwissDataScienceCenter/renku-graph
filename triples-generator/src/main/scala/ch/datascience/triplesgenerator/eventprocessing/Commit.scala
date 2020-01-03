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

package ch.datascience.triplesgenerator.eventprocessing

import ch.datascience.graph.model.events._

private sealed trait Commit extends Product with Serializable {
  val id:      CommitId
  val project: Project
}

private object Commit {

  final case class CommitWithParent(
      id:       CommitId,
      parentId: CommitId,
      project:  Project
  ) extends Commit

  final case class CommitWithoutParent(
      id:      CommitId,
      project: Project
  ) extends Commit

  implicit class CommitOps(commit: Commit) {
    lazy val commitEventId: CommitEventId = CommitEventId(commit.id, commit.project.id)
  }
}
