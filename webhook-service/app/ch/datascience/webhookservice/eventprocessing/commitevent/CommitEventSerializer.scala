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
import ch.datascience.graph.model.events._
import io.circe.Json
import javax.inject.Singleton

import scala.language.higherKinds
import scala.util.Try

class CommitEventSerializer[Interpretation[_]](implicit ME: MonadError[Interpretation, Throwable]) {

  def serialiseToJsonString(commitEvent: CommitEvent): Interpretation[String] =
    ME.fromTry {
      Try {
        Json
          .obj(
            "id"            -> Json.fromString(commitEvent.id.value),
            "message"       -> Json.fromString(commitEvent.message.value),
            "committedDate" -> Json.fromString(commitEvent.committedDate.toString),
            "pushUser"      -> toJson(commitEvent.pushUser),
            "author" -> Json.obj(
              "username" -> Json.fromString(commitEvent.author.username.value),
              "email"    -> Json.fromString(commitEvent.author.email.value)
            ),
            "committer" -> Json.obj(
              "username" -> Json.fromString(commitEvent.committer.username.value),
              "email"    -> Json.fromString(commitEvent.committer.email.value)
            ),
            "parents" -> Json.arr(commitEvent.parents.map(parentId => Json.fromString(parentId.value)): _*),
            "project" -> Json.obj(
              "id"   -> Json.fromInt(commitEvent.project.id.value),
              "path" -> Json.fromString(commitEvent.project.path.value)
            )
          )
          .noSpaces
      }
    }

  private def toJson(pushUser: PushUser): Json =
    Json.obj(
      Seq(
        Some("userId"   -> Json.fromInt(pushUser.userId.value)),
        Some("username" -> Json.fromString(pushUser.username.value)),
        pushUser.maybeEmail.map(email => "email" -> Json.fromString(email.value))
      ).flatten: _*
    )
}

@Singleton
private class IOCommitEventSerializer extends CommitEventSerializer[IO]
