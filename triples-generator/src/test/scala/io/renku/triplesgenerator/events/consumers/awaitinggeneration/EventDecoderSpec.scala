/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration

import io.circe._
import io.renku.events.consumers.Project
import io.renku.events.EventRequestContent
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.jsons
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.{EventBody, EventId}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues

class EventDecoderSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "toCommitEvents" should {

    "produce a CommitEvent if the Json string can be successfully deserialized" in new TestCase {
      EventDecoder
        .decode(EventRequestContent.WithPayload(jsons.generateOne, eventBody))
        .value shouldBe CommitEvent(EventId(commitId.value), Project(projectId, projectPath), commitId)
    }

    "fail if parsing fails" in new TestCase {

      val result = EventDecoder.decode(EventRequestContent.WithPayload(jsons.generateOne, EventBody("{")))

      result.left.value                                         shouldBe a[ParsingFailure]
      result.left.value.getMessage                              shouldBe "CommitEvent cannot be deserialised: '{'"
      result.left.value.asInstanceOf[ParsingFailure].underlying shouldBe a[ParsingFailure]
    }

    "fail if decoding fails" in new TestCase {

      val result = EventDecoder.decode(EventRequestContent.WithPayload(jsons.generateOne, EventBody("{}")))

      result.left.value                                       shouldBe a[DecodingFailure]
      result.left.value.asInstanceOf[DecodingFailure].message shouldBe "CommitEvent cannot be deserialised: '{}'"
    }
  }

  private trait TestCase {
    val commitId    = commitIds.generateOne
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    lazy val eventBody: EventBody = EventBody {
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
