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
import cats.implicits._
import ch.datascience.crypto.AesCrypto.Secret
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators.commitEvents
import ch.datascience.graph.events.crypto.HookAccessTokenCrypto
import io.circe.Json
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import ch.datascience.generators.Generators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.HookAccessToken
import ch.datascience.graph.events.GraphCommonsGenerators._

import scala.util.{Failure, Success, Try}

class CommitEventSerializerSpec extends WordSpec with MockFactory {

  "serialiseToJsonString" should {

    "return a single line json if serialization is successful" in new TestCase {

      val serializedHookAccessToken = serializedHookAccessTokens.generateOne
      (hookAccessTokenCrypto
        .encrypt(_: HookAccessToken))
        .expects(commitEvent.hookAccessToken)
        .returning(context.pure(serializedHookAccessToken))

      serializer.serialiseToJsonString(commitEvent) shouldBe Success(
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
            "parents" -> Json.fromValues(commitEvent.parents.map(parent => Json.fromString(parent.value))),
            "project" -> Json.obj(
              "id"   -> Json.fromInt(commitEvent.project.id.value),
              "path" -> Json.fromString(commitEvent.project.path.value)
            ),
            "hookAccessToken" -> Json.fromString(serializedHookAccessToken.value)
          )
          .noSpaces
      )
    }

    "fail if Hook Access Token serialization fails" in new TestCase {

      val exception = exceptions.generateOne
      (hookAccessTokenCrypto
        .encrypt(_: HookAccessToken))
        .expects(commitEvent.hookAccessToken)
        .returning(context.raiseError(exception))

      serializer.serialiseToJsonString(commitEvent) shouldBe Failure(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitEvent = commitEvents.generateOne

    class TryHookAccessTokenCrypto(secret: Secret) extends HookAccessTokenCrypto[Try](secret)
    val hookAccessTokenCrypto = mock[TryHookAccessTokenCrypto]
    val serializer            = new CommitEventSerializer[Try](hookAccessTokenCrypto)
  }
}
