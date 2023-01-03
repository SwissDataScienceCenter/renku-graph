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

package io.renku.webhookservice.eventstatus

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{fixed, nonEmptyStrings}
import io.renku.graph.eventlog.EventLogClient
import io.renku.graph.model.EventContentGenerators.eventInfos
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.events.EventInfo
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class StatusInfoFinderSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "findStatusInfo" should {

    "return activated project StatusInfo with progress based on the latest event's status" in new TestCase {

      val eventInfo = eventInfos(projectIdGen = fixed(projectId)).generateOne
      givenGetEvents(projectId, returning = EventLogClient.Result.Success(List(eventInfo)).pure[Try])

      fetcher.findStatusInfo(projectId).value shouldBe StatusInfo.activated(eventInfo.status).asRight.pure[Try]
    }

    "return non-activated project StatusInfo when no events found for the project" in new TestCase {

      givenGetEvents(projectId, returning = EventLogClient.Result.Success(List.empty).pure[Try])

      fetcher.findStatusInfo(projectId).value shouldBe StatusInfo.NotActivated.asRight.pure[Try]
    }

    "return an Exception if EL responds with a failure" in new TestCase {

      val message = nonEmptyStrings().generateOne
      givenGetEvents(projectId, returning = EventLogClient.Result.failure(message).pure[Try])

      val Success(result) = fetcher.findStatusInfo(projectId).value

      result                       shouldBe a[Left[_, _]]
      result.leftMap(_.getMessage) shouldBe message.asLeft
    }

    "return an Exception if EL responds with an unavailable" in new TestCase {

      givenGetEvents(projectId, returning = EventLogClient.Result.unavailable.pure[Try])

      val Success(result) = fetcher.findStatusInfo(projectId).value

      result shouldBe a[Left[_, _]]
      result shouldBe EventLogClient.Result.Unavailable.asLeft
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    private val eventLogClient = mock[EventLogClient[Try]]
    val fetcher                = new StatusInfoFinderImpl[Try](eventLogClient)

    def givenGetEvents(projectId: projects.GitLabId, returning: Try[EventLogClient.Result[List[EventInfo]]]) =
      (eventLogClient.getEvents _)
        .expects(
          EventLogClient.SearchCriteria
            .forProject(projectId)
            .withPerPage(PerPage(1))
            .sortBy(EventLogClient.SearchCriteria.Sort.EventDateDesc)
        )
        .returning(returning)
  }
}
