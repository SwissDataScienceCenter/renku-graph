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

package io.renku.knowledgegraph.projects.update

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.eventlog.api.EventLogClient.{Result, SearchCriteria}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.graph.model.EventContentGenerators.eventInfos
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.events.EventStatus.{GenerationNonRecoverableFailure, TransformationNonRecoverableFailure}
import io.renku.graph.model.events.{EventInfo, EventStatus}
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import io.renku.knowledgegraph.projects.update.ProvisioningStatusFinder.ProvisioningStatus.{Healthy, Unhealthy}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProvisioningStatusFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with OptionValues {

  EventStatus.all - GenerationNonRecoverableFailure - TransformationNonRecoverableFailure foreach { status =>
    it should show"return Healthy if the latest commit is found and it's in $status status" in {

      val project = projectSlugs.generateOne

      givenELClient(project,
                    returning = Result.success(List(eventInfos(project).generateOne.copy(status = status))).pure[IO]
      )

      finder.checkHealthy(project).asserting(_ shouldBe Healthy)
    }
  }

  GenerationNonRecoverableFailure :: TransformationNonRecoverableFailure :: Nil foreach { status =>
    it should show"return Unhealthy if the latest commit is found and it's in $status status" in {

      val project = projectSlugs.generateOne

      givenELClient(project,
                    returning = Result.success(List(eventInfos(project).generateOne.copy(status = status))).pure[IO]
      )

      finder.checkHealthy(project).asserting(_ shouldBe Unhealthy(status))
    }
  }

  it should "return Healthy if the latest commit is not found" in {

    val project = projectSlugs.generateOne

    givenELClient(project, returning = Result.success(Nil).pure[IO])

    finder.checkHealthy(project).asserting(_ shouldBe Healthy)
  }

  it should "fail if call to the EL returns a failure" in {

    val project = projectSlugs.generateOne

    val failure = Result.failure(nonEmptyStrings().generateOne)
    givenELClient(project, returning = failure.pure[IO])

    finder.checkHealthy(project).assertThrowsError[Exception](_ shouldBe failure)
  }

  it should "fail if call to the EL fails" in {

    val project = projectSlugs.generateOne

    val failure = exceptions.generateOne
    givenELClient(project, returning = failure.raiseError[IO, Nothing])

    finder.checkHealthy(project).assertThrowsError[Exception](_ shouldBe failure)
  }

  private val elClient    = mock[EventLogClient[IO]]
  private lazy val finder = new ProvisioningStatusFinderImpl[IO](elClient)

  private def givenELClient(project: projects.Slug, returning: IO[EventLogClient.Result[List[EventInfo]]]) =
    (elClient.getEvents _)
      .expects(SearchCriteria.forProject(project).withPerPage(PerPage(1)).sortBy(SearchCriteria.Sort.EventDateDesc))
      .returning(returning)
}
