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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.EventsGenerators.commitEvents
import io.circe.Json
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import ch.datascience.graph.events.EventsGenerators._

import scala.util.{Success, Try}

class CommitEventSerializerSpec extends WordSpec with MockFactory {

  "serialiseToJsonString" should {

    "return a single line json if serialization is successful and there's pushUser's email" in new TestCase {

      val pushUserEmail = emails.generateOne
      val commitEvent = {
        val event = commitEvents.generateOne
        event.copy(
          pushUser = event.pushUser.copy(maybeEmail = Some(pushUserEmail))
        )
      }

      serializer.serialiseToJsonString(commitEvent) shouldBe Success(
        Json
          .obj(
            "id"            -> Json.fromString(commitEvent.id.value),
            "message"       -> Json.fromString(commitEvent.message.value),
            "committedDate" -> Json.fromString(commitEvent.committedDate.toString),
            "pushUser" -> Json.obj(
              "userId"   -> Json.fromInt(commitEvent.pushUser.userId.value),
              "username" -> Json.fromString(commitEvent.pushUser.username.value),
              "email"    -> Json.fromString(pushUserEmail.value)
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
            )
          )
          .noSpaces
      )
    }

    "return a single line json if serialization is successful and there's no pushUser's email" in new TestCase {

      val commitEvent = {
        val event = commitEvents.generateOne
        event.copy(
          pushUser = event.pushUser.copy(maybeEmail = None)
        )
      }

      serializer.serialiseToJsonString(commitEvent) shouldBe Success(
        Json
          .obj(
            "id"            -> Json.fromString(commitEvent.id.value),
            "message"       -> Json.fromString(commitEvent.message.value),
            "committedDate" -> Json.fromString(commitEvent.committedDate.toString),
            "pushUser" -> Json.obj(
              "userId"   -> Json.fromInt(commitEvent.pushUser.userId.value),
              "username" -> Json.fromString(commitEvent.pushUser.username.value)
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
            )
          )
          .noSpaces
      )
    }
  }

  private trait TestCase {
    val serializer = new CommitEventSerializer[Try]()
  }
}
