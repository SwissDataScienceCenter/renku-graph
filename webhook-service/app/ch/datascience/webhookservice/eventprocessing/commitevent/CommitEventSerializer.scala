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
import cats.implicits._
import ch.datascience.graph.events._
import ch.datascience.graph.events.crypto.{HookAccessTokenCrypto, IOHookAccessTokenCrypto}
import io.circe.Json
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.Try

class CommitEventSerializer[Interpretation[_]](
    hookAccessTokenCrypto: HookAccessTokenCrypto[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import hookAccessTokenCrypto._

  def serialiseToJsonString(commitEvent: CommitEvent): Interpretation[String] =
    for {
      serializedHookAccessToken <- encrypt(commitEvent.hookAccessToken)
      serializedCommitEvent     <- serialise(commitEvent, serializedHookAccessToken)
    } yield serializedCommitEvent

  private def serialise(commitEvent:               CommitEvent,
                        serializedHookAccessToken: SerializedHookAccessToken): Interpretation[String] =
    ME.fromTry {
      Try {
        Json
          .obj(
            "id"            -> Json.fromString(commitEvent.id.value),
            "message"       -> Json.fromString(commitEvent.message.value),
            "committedDate" -> Json.fromString(commitEvent.committedDate.toString),
            "pushUser" -> Json.obj(
              "userId"   -> Json.fromInt(commitEvent.pushUser.userId.value),
              "username" -> Json.fromString(commitEvent.pushUser.username.value),
              "email"    -> Json.fromString(commitEvent.pushUser.email.value)
            ),
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
            ),
            "hookAccessToken" -> Json.fromString(serializedHookAccessToken.value)
          )
          .noSpaces
      }
    }
}

@Singleton
private class IOCommitEventSerializer @Inject()(
    hookAccessTokenCrypto: IOHookAccessTokenCrypto
) extends CommitEventSerializer[IO](hookAccessTokenCrypto)
