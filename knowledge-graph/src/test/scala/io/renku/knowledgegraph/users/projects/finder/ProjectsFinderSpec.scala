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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria
import Endpoint.Criteria.Filters
import Endpoint.Criteria.Filters._
import ProjectsFinder.nameOrdering
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model._
import io.renku.http.rest.paging.{PagingRequest, PagingResponse}
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ProjectsFinderSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "findProjects" should {

    "combine results from both TS and GL favouring TS projects when having the same path" in new TestCase {

      val criteria      = criterias.generateOne.copy(filters = Filters(ActivationState.All))
      val commonProject = notActivatedProjects.generateOne

      val tsProjects = commonProject.toActivated :: activatedProjects.generateList(max = 4)
      givenTSFinding(criteria, returning = tsProjects.pure[Try])

      val glProjects = notActivatedProjects.generateList(max = 4)
      givenGLFinding(criteria, returning = (commonProject :: glProjects).pure[Try])

      val allSortedProjects = (tsProjects ::: glProjects).sortBy(_.name)
      val expectedProjects  = modelProjects.generateFixedSizeList(allSortedProjects.size)
      givenGLCreatorsNamesAdding(criteria, allSortedProjects, returning = expectedProjects.pure[Try])

      val actualResults = finder.findProjects(criteria)

      actualResults.success.value.results          shouldBe expectedProjects
      actualResults.success.value.pagingInfo.total shouldBe Total(expectedProjects.size)
    }

    "add person names only to the requested page" in new TestCase {

      val paging   = PagingRequest(Page(2), PerPage(5))
      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All), paging = paging)
      val projects = modelProjects.generateList(
        min = paging.perPage.value + 1,
        max = paging.perPage.value * paging.page.value + 1
      )

      val tsProjects = projects.collect { case p: model.Project.Activated => p }
      givenTSFinding(criteria, returning = tsProjects.pure[Try])

      val glProjects = projects.collect { case p: model.Project.NotActivated => p }
      givenGLFinding(criteria, returning = glProjects.pure[Try])

      val allSortedProjects = (tsProjects ::: glProjects).sortBy(_.name)
      val pageProjects =
        PagingResponse.from[Try, model.Project](allSortedProjects, paging).fold(throw _, identity).results
      val enrichedPageProjects = modelProjects.generateFixedSizeList(pageProjects.size)
      givenGLCreatorsNamesAdding(criteria, pageProjects, returning = enrichedPageProjects.pure[Try])

      val actualResults = finder.findProjects(criteria)

      actualResults.success.value.results          shouldBe enrichedPageProjects
      actualResults.success.value.pagingInfo.total shouldBe Total(allSortedProjects.size)
    }

    "return not activated projects only if ActivationState is set to 'NotActivated'" in new TestCase {

      val criteria      = criterias.generateOne.copy(filters = Filters(ActivationState.NotActivated))
      val commonProject = notActivatedProjects.generateOne

      val tsProjects = commonProject.toActivated :: activatedProjects.generateList(max = 4)
      givenTSFinding(criteria, returning = tsProjects.pure[Try])

      val glProjects = notActivatedProjects.generateNonEmptyList(max = 4).toList
      givenGLFinding(criteria, returning = (commonProject :: glProjects).pure[Try])

      val allSortedProjects = glProjects.sortBy(_.name)
      val expectedProjects  = modelProjects.generateFixedSizeList(allSortedProjects.size)
      givenGLCreatorsNamesAdding(criteria, allSortedProjects, returning = expectedProjects.pure[Try])

      val actualResults = finder.findProjects(criteria)

      actualResults.success.value.results          shouldBe expectedProjects
      actualResults.success.value.pagingInfo.total shouldBe Total(expectedProjects.size)
    }

    "return activated projects only if ActivationState is set to 'Activated'" in new TestCase {

      val criteria      = criterias.generateOne.copy(filters = Filters(ActivationState.Activated))
      val commonProject = notActivatedProjects.generateOne

      val tsProjects = commonProject.toActivated :: activatedProjects.generateList(max = 4)
      givenTSFinding(criteria, returning = tsProjects.pure[Try])

      val glProjects = notActivatedProjects.generateList(max = 4)
      givenGLFinding(criteria, returning = (commonProject :: glProjects).pure[Try])

      val allSortedProjects = tsProjects.sortBy(_.name)
      val expectedProjects  = modelProjects.generateFixedSizeList(allSortedProjects.size)
      givenGLCreatorsNamesAdding(criteria, allSortedProjects, returning = expectedProjects.pure[Try])

      val actualResults = finder.findProjects(criteria)

      actualResults.success.value.results          shouldBe expectedProjects
      actualResults.success.value.pagingInfo.total shouldBe Total(expectedProjects.size)
    }

    "fail if finding projects in TS fails" in new TestCase {

      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All))

      val exception = exceptions.generateOne
      givenTSFinding(criteria, returning = exception.raiseError[Try, List[model.Project.Activated]])

      givenGLFinding(criteria, returning = List.empty.pure[Try])

      finder.findProjects(criteria) shouldBe exception.raiseError[Try, List[model.Project]]
    }

    "fail if finding projects in GL fails" in new TestCase {

      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All))

      givenTSFinding(criteria, returning = List.empty.pure[Try])

      val exception = exceptions.generateOne
      givenGLFinding(criteria, returning = exception.raiseError[Try, List[model.Project.NotActivated]])

      finder.findProjects(criteria) shouldBe exception.raiseError[Try, List[model.Project]]
    }

    "return an empty List if no projects found" in new TestCase {

      val criteria = criterias.generateOne.copy(filters = Filters(ActivationState.All))

      givenTSFinding(criteria, returning = List.empty.pure[Try])
      givenGLFinding(criteria, returning = List.empty.pure[Try])
      givenGLCreatorsNamesAdding(criteria, List.empty, returning = List.empty.pure[Try])

      val actualResults = finder.findProjects(criteria)

      actualResults.success.value.results          shouldBe Nil
      actualResults.success.value.pagingInfo.total shouldBe Total(0)
    }
  }

  "nameOrdering" should {

    "sort names in case insensitive way" in {
      val a = projects.Name("a")
      val b = projects.Name("B")
      List(b, a).sorted shouldBe List(a, b)
    }
  }

  private trait TestCase {
    private val tsProjectsFinder     = mock[TSProjectFinder[Try]]
    private val glProjectsFinder     = mock[GLProjectFinder[Try]]
    private val glCreatorsNamesAdder = mock[GLCreatorsNamesAdder[Try]]
    val finder = new ProjectsFinderImpl[Try](tsProjectsFinder, glProjectsFinder, glCreatorsNamesAdder)

    def givenTSFinding(criteria: Criteria, returning: Try[List[model.Project.Activated]]) =
      (tsProjectsFinder.findProjectsInTS _)
        .expects(criteria)
        .returning(returning)

    def givenGLFinding(criteria: Criteria, returning: Try[List[model.Project.NotActivated]]) =
      (glProjectsFinder.findProjectsInGL _)
        .expects(criteria)
        .returning(returning)

    def givenGLCreatorsNamesAdding(criteria:  Criteria,
                                   projects:  List[model.Project],
                                   returning: Try[List[model.Project]]
    ) = (glCreatorsNamesAdder
      .addCreatorsNames(_: Criteria)(_: List[model.Project]))
      .expects(criteria, projects)
      .returning(returning)
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
