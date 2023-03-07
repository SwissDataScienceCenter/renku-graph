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

package io.renku.eventlog.events.consumers
package commitsyncrequest

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "createHandlingDefinition.decode" should {
    "decode project id and path" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData = Json.obj(
        "project" -> Json.obj(
          "id"   -> 42.asJson,
          "path" -> "the/project".asJson
        )
      )
      definition.decode(EventRequestContent(eventData)) shouldBe Project(42, "the/project").asRight
    }

    "fail on malformed event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Json.obj("invalid" -> true.asJson)
      definition.decode(EventRequestContent(eventData)).isLeft shouldBe true
    }
  }

  "createHandlingDefinition.process" should {
    "call commitSyncForcer" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (commitSyncForcer.forceCommitSync _).expects(*, *).returning(IO.unit)
      definition.process(Project(42, "the/project")).unsafeRunSync() shouldBe ()
    }
  }

  "createHandlingDefinition" should {
    "not define onRelease and precondition" in new TestCase {
      val definition = handler.createHandlingDefinition()
      definition.onRelease                    shouldBe None
      definition.precondition.unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val commitSyncForcer = mock[CommitSyncForcer[IO]]
    val handler          = new EventHandler[IO](commitSyncForcer)
  }

  private implicit lazy val eventEncoder: Encoder[(projects.GitLabId, projects.Path)] =
    Encoder.instance[(projects.GitLabId, projects.Path)] { case (id, path) =>
      json"""{
        "categoryName": "COMMIT_SYNC_REQUEST",
        "project": {
          "id":   ${id.value},
          "path": ${path.value}
        }
      }"""
    }
}
