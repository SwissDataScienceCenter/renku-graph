/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.delete

import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.eventlog.api.EventLogClient.Result.{Failure, Success}
import io.renku.eventlog.api.EventLogClient.{Result, SearchCriteria}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.graph.model.EventContentGenerators.eventInfos
import io.renku.graph.model.events.EventInfo
import io.renku.http.rest.paging.model.PerPage
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ELProjectFinderSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "findProject" should {

    "return a project if found in EL" in new TestCase {

      givenELProjectFinding(returning =
        Success(eventInfos(project.slug, project.id).generateFixedSizeList(ofSize = 1)).pure[Try]
      )

      finder.findProject(project.slug).success.value shouldBe Some(project)
    }

    "return no project if not found in EL" in new TestCase {

      givenELProjectFinding(returning = Success(List.empty[EventInfo]).pure[Try])

      finder.findProject(project.slug).success.value shouldBe None
    }

    "fail if finding project return an error" in new TestCase {

      val failure = Failure(nonEmptyStrings().generateOne)
      givenELProjectFinding(returning = failure.pure[Try])

      finder.findProject(project.slug).failure.exception shouldBe failure
    }

    "fail if finding project fails" in new TestCase {

      val failure = exceptions.generateOne
      givenELProjectFinding(returning = failure.raiseError[Try, Result[List[EventInfo]]])

      finder.findProject(project.slug).failure.exception shouldBe failure
    }
  }

  private trait TestCase {

    val project = consumerProjects.generateOne

    private val elClient = mock[EventLogClient[Try]]
    val finder           = new ELProjectFinderImpl[Try](elClient)

    def givenELProjectFinding(returning: Try[EventLogClient.Result[List[EventInfo]]]) =
      (elClient.getEvents _)
        .expects(SearchCriteria.forProject(project.slug).withPerPage(PerPage(1)))
        .returning(returning)
  }
}
