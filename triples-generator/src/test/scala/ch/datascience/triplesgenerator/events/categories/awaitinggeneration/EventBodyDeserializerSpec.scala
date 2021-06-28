/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.syntax.all._
import ch.datascience.events.consumers.Project
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{EventBody, EventId}
import io.circe._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class EventBodyDeserializerSpec extends AnyWordSpec with should.Matchers {

  "toCommitEvents" should {

    "produce a CommitEvent if the Json string can be successfully deserialized" in new TestCase {
      deserializer.toCommitEvent(commitEvent) shouldBe CommitEvent(
        EventId(commitId.value),
        Project(projectId, projectPath),
        commitId
      ).pure[Try]
    }

    "fail if parsing fails" in new TestCase {
      val Failure(ParsingFailure(message, underlying)) = deserializer.toCommitEvent(EventBody("{"))

      message    shouldBe "CommitEvent cannot be deserialised: '{'"
      underlying shouldBe a[ParsingFailure]
    }

    "fail if decoding fails" in new TestCase {
      val Failure(DecodingFailure(message, _)) = deserializer.toCommitEvent(EventBody("{}"))

      message shouldBe "CommitEvent cannot be deserialised: '{}'"
    }
  }

  private trait TestCase {
    val commitId    = commitIds.generateOne
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val deserializer = new EventBodyDeserializerImpl[Try]

    def commitEvent: EventBody = EventBody {
      Json
        .obj(
          "id"      -> Json.fromString(commitId.value),
          "parents" -> Json.arr(commitIds.generateList().map(_.value).map(Json.fromString): _*),
          "project" -> Json.obj(
            "id"   -> Json.fromInt(projectId.value),
            "path" -> Json.fromString(projectPath.value)
          )
        )
        .noSpaces
    }
  }
}
