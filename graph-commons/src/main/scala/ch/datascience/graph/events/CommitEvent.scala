/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

import java.time.Instant

import ch.datascience.tinytypes.constraints.{ GitSha, NonBlank }
import ch.datascience.tinytypes.{ TinyType, TinyTypeFactory }

case class CommitEvent(
    id:        CommitId,
    message:   String,
    timestamp: Instant,
    pushUser:  PushUser,
    author:    User,
    committer: User,
    parents:   Seq[CommitId],
    project:   Project,
    added:     Seq[GitFile],
    modified:  Seq[GitFile],
    removed:   Seq[GitFile]
)

class CommitId private ( val value: String ) extends AnyVal with TinyType[String]
object CommitId
  extends TinyTypeFactory[String, CommitId]( new CommitId( _ ) )
  with GitSha

class GitFile private ( val value: String ) extends AnyVal with TinyType[String]
object GitFile
  extends TinyTypeFactory[String, GitFile]( new GitFile( _ ) )
  with NonBlank
