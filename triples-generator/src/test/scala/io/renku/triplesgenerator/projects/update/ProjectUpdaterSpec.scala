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

package io.renku.triplesgenerator.projects.update

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.triplesgenerator.api.Generators.projectUpdatesGen
import io.renku.triplesgenerator.api.ProjectUpdates
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class ProjectUpdaterSpec extends AnyFlatSpec with should.Matchers with TryValues with MockFactory {

  it should "check if project exists and if not return NotExists" in {

    val slug = projectSlugs.generateOne
    givenProjectExistenceChecking(slug, returning = false.pure[Try])

    val updates = projectUpdatesGen.generateOne

    updater.updateProject(slug, updates).success.value shouldBe ProjectUpdater.Result.NotExists
  }

  it should "check if project exists, prepare update queries, execute them and return Updated" in {

    val slug = projectSlugs.generateOne
    givenProjectExistenceChecking(slug, returning = true.pure[Try])

    val updates = projectUpdatesGen.generateOne
    val queries = sparqlQueries.generateList()
    givenUpdatesCalculation(slug, updates, returning = queries.pure[Try])

    queries foreach givenRunningQuerySucceeds

    updater.updateProject(slug, updates).success.value shouldBe ProjectUpdater.Result.Updated
  }

  private lazy val projectExistenceChecker = mock[ProjectExistenceChecker[Try]]
  private lazy val updateQueriesCalculator = mock[UpdateQueriesCalculator[Try]]
  private lazy val tsClient                = mock[TSClient[Try]]
  private lazy val updater = new ProjectUpdaterImpl[Try](projectExistenceChecker, updateQueriesCalculator, tsClient)

  private def givenProjectExistenceChecking(slug: projects.Slug, returning: Try[Boolean]) =
    (projectExistenceChecker.checkExists _)
      .expects(slug)
      .returning(returning)

  private def givenUpdatesCalculation(slug: projects.Slug, updates: ProjectUpdates, returning: Try[List[SparqlQuery]]) =
    (updateQueriesCalculator.calculateUpdateQueries _)
      .expects(slug, updates)
      .returning(returning)

  private def givenRunningQuerySucceeds(query: SparqlQuery) =
    (tsClient.updateWithNoResult _)
      .expects(query)
      .returning(().pure[Try])
}
