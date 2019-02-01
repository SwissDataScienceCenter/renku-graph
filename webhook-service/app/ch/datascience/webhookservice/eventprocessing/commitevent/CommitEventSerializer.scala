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

package ch.datascience.webhookservice.eventprocessing.commitevent

import cats.MonadError
import cats.effect.IO
import ch.datascience.graph.events.{CommitEvent, Project, PushUser, User}
import javax.inject.Singleton
import play.api.libs.json.{Json, Writes}

import scala.util.Try

class CommitEventSerializer[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable]) {

  private implicit val userWrites:     Writes[User]        = Json.writes[User]
  private implicit val pushUserWrites: Writes[PushUser]    = Json.writes[PushUser]
  private implicit val projectWrites:  Writes[Project]     = Json.writes[Project]
  private val commitEventWrites:       Writes[CommitEvent] = Json.writes[CommitEvent]

  def serialiseToJsonString(commitEvent: CommitEvent): Interpretation[String] = ME.fromTry {
    Try {
      Json.stringify(commitEventWrites.writes(commitEvent))
    }
  }
}

@Singleton
private class IOCommitEventSerializer extends CommitEventSerializer[IO]
