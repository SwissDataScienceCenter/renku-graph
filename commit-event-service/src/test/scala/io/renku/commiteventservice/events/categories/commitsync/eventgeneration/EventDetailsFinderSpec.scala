/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.commiteventservice.events.categories.common.CommitWithParents
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.Created
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDetailsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "getEventDetails" should {

    "return commit info when event log responds with Ok" in new TestCase {

      val commit = commitWithParentsGen.generateOne

      stubFor {
        get(s"/events/${event.id}/${event.project.id}")
          .willReturn(okJson(commit.asJson.noSpaces))
      }

      eventDetailsFinder.getEventDetails(event.project.id, event.id).unsafeRunSync() shouldBe Some(commit)
    }

    "return None when event log responds with NotFound" in new TestCase {

      stubFor {
        get(s"/events/${event.id}/${event.project.id}")
          .willReturn(notFound())
      }

      eventDetailsFinder.getEventDetails(event.project.id, event.id).unsafeRunSync() shouldBe None
    }

    "fail when event log responds with other statuses" in new TestCase {

      stubFor {
        get(s"/events/${event.id}/${event.project.id}")
          .willReturn(aResponse().withStatus(Created.code))
      }
      intercept[Exception] {
        eventDetailsFinder.getEventDetails(event.project.id, event.id).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger()
    val event              = newCommitEvents.generateOne
    val eventLogUrl        = EventLogUrl(externalServiceBaseUrl)
    val eventDetailsFinder = new EventDetailsFinderImpl[IO](eventLogUrl)
  }

  private lazy val commitWithParentsGen: Gen[CommitWithParents] = for {
    commitId  <- commitIds
    projectId <- projectIds
    parents   <- commitIds.toGeneratorOfList()
  } yield CommitWithParents(commitId, projectId, parents)

  private implicit val encoder: Encoder[CommitWithParents] = Encoder.instance {
    case CommitWithParents(id, projectId, parents) => json"""{
      "id": ${id.value},
      "project": {
        "id": ${projectId.value}
      },
      "body": ${json"""{"parents": ${parents.map(_.value)}}""".noSpaces}
    }"""
  }
}
