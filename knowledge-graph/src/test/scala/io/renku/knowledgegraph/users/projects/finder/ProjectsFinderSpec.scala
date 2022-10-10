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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria.Filters
import Endpoint.Criteria.Filters._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.http.rest.paging.model.Total
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ProjectsFinderSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "findProjects" should {

    "combine results from both TS and GitLab favouring TS projects when having the same path" in new TestCase {

      val criteria      = criterias.generateOne.copy(filters = Filters(ActivationState.All))
      val commonProject = notActivatedProjects.generateOne

      val tsProjects = commonProject.toActivated :: activatedProjects.generateList(max = 4)
      (tsProjectsFinder.findProjectsInTS _).expects(criteria).returning(tsProjects.pure[Try])

      val glProjects = notActivatedProjects.generateList(max = 4)
      (glProjectsFinder.findProjectsInGL _)
        .expects(criteria)
        .returning((commonProject :: glProjects).pure[Try])

      val Success(actualResults) = finder.findProjects(criteria)

      val expectedProjects = (tsProjects ::: glProjects).sortBy(_.name)
      actualResults.results          shouldBe expectedProjects
      actualResults.pagingInfo.total shouldBe Total(expectedProjects.size)
    }

    "return not activated projects only if ActivationState is set to 'NotActivated'" in new TestCase {

      val criteria      = criterias.generateOne.copy(filters = Filters(ActivationState.NotActivated))
      val commonProject = notActivatedProjects.generateOne

      val tsProjects = commonProject.toActivated :: activatedProjects.generateList(max = 4)
      (tsProjectsFinder.findProjectsInTS _).expects(criteria).returning(tsProjects.pure[Try])

      val glProjects = notActivatedProjects.generateList(max = 4)
      (glProjectsFinder.findProjectsInGL _)
        .expects(criteria)
        .returning((commonProject :: glProjects).pure[Try])

      val Success(actualResults) = finder.findProjects(criteria)

      val expectedProjects = glProjects.sortBy(_.name)
      actualResults.results          shouldBe expectedProjects
      actualResults.pagingInfo.total shouldBe Total(expectedProjects.size)
    }

    "return activated projects only if ActivationState is set to 'Activated'" in new TestCase {

      val criteria      = criterias.generateOne.copy(filters = Filters(ActivationState.Activated))
      val commonProject = notActivatedProjects.generateOne

      val tsProjects = commonProject.toActivated :: activatedProjects.generateList(max = 4)
      (tsProjectsFinder.findProjectsInTS _).expects(criteria).returning(tsProjects.pure[Try])

      val glProjects = notActivatedProjects.generateList(max = 4)
      (glProjectsFinder.findProjectsInGL _)
        .expects(criteria)
        .returning((commonProject :: glProjects).pure[Try])

      val Success(actualResults) = finder.findProjects(criteria)

      val expectedProjects = tsProjects.sortBy(_.name)
      actualResults.results          shouldBe expectedProjects
      actualResults.pagingInfo.total shouldBe Total(expectedProjects.size)
    }

    "fail if finding projects in TS fails" in new TestCase {

      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All))

      val exception = exceptions.generateOne
      (tsProjectsFinder.findProjectsInTS _)
        .expects(criteria)
        .returning(exception.raiseError[Try, List[model.Project.Activated]])

      (glProjectsFinder.findProjectsInGL _).expects(criteria).returning(List.empty.pure[Try])

      finder.findProjects(criteria) shouldBe exception.raiseError[Try, List[model.Project]]
    }

    "fail if finding projects in GL fails" in new TestCase {

      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All))

      (tsProjectsFinder.findProjectsInTS _).expects(criteria).returning(List.empty.pure[Try])

      val exception = exceptions.generateOne
      (glProjectsFinder.findProjectsInGL _)
        .expects(criteria)
        .returning(exception.raiseError[Try, List[model.Project.NotActivated]])

      finder.findProjects(criteria) shouldBe exception.raiseError[Try, List[model.Project]]
    }

    "return an empty List if no projects found" in new TestCase {

      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All))

      (tsProjectsFinder.findProjectsInTS _).expects(criteria).returning(List.empty.pure[Try])
      (glProjectsFinder.findProjectsInGL _).expects(criteria).returning(List.empty.pure[Try])

      val Success(actualResults) = finder.findProjects(criteria)

      actualResults.results          shouldBe Nil
      actualResults.pagingInfo.total shouldBe Total(0)
    }
  }

  private trait TestCase {
    val tsProjectsFinder = mock[TSProjectFinder[Try]]
    val glProjectsFinder = mock[GLProjectFinder[Try]]
    val finder           = new ProjectsFinderImpl[Try](tsProjectsFinder, glProjectsFinder)
  }

  private implicit class NotActivatedOps(project: model.Project.NotActivated) {

    lazy val toActivated: model.Project.Activated = model.Project.Activated(
      project.name,
      project.path,
      project.visibility,
      project.dateCreated,
      project.maybeCreator,
      project.keywords,
      project.maybeDesc
    )
  }
}
