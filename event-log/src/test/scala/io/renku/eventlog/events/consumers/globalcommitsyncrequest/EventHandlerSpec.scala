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
package globalcommitsyncrequest

import cats.effect.IO
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._
import io.renku.events.EventRequestContent
import io.renku.events.consumers.Project
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  "createHandlingDefinition.decode" should {
    "decode a valid event successfully" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Project(42, "the/project")
      definition.decode(EventRequestContent(eventData.asJson)) shouldBe Right(eventData)
    }

    "fail on invalid event data" in new TestCase {
      val definition = handler.createHandlingDefinition()
      val eventData  = Json.obj("invalid" -> true.asJson)
      definition.decode(EventRequestContent(eventData)).isLeft shouldBe true
    }
  }

  "createHandlingDefinition.process" should {
    "call to EventPersister" in new TestCase {
      val definition = handler.createHandlingDefinition()
      (globalCommitSyncForcer.moveGlobalCommitSync _).expects(*, *).returning(IO.unit)
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
    val globalCommitSyncForcer = mock[GlobalCommitSyncForcer[IO]]
    val handler                = new EventHandler[IO](globalCommitSyncForcer)
  }

  private implicit lazy val eventEncoder: Encoder[Project] =
    Encoder.instance[Project] { case Project(id, slug) =>
      json"""{
        "categoryName": "GLOBAL_COMMIT_SYNC_REQUEST",
        "project": {
          "id":   $id,
          "slug": $slug
        }
      }"""
    }
}
