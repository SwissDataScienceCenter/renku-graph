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

package io.renku.triplesgenerator.projects
package update

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.lock.Lock
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.api.Generators.projectUpdatesGen
import io.renku.triplesgenerator.api.ProjectUpdates
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectUpdaterSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "check if project exists and if not return NotExists" in {

    val slug = projectSlugs.generateOne
    givenProjectExistenceChecking(slug, returning = false.pure[IO])

    val updates = projectUpdatesGen.generateOne

    updater.updateProject(slug, updates).asserting(_ shouldBe ProjectUpdater.Result.NotExists)
  }

  it should "check if project exists, prepare update queries, execute them and return Updated" in {

    val slug = projectSlugs.generateOne
    givenProjectExistenceChecking(slug, returning = true.pure[IO])

    val updates = projectUpdatesGen.generateOne
    val queries = sparqlQueries.generateList()
    givenUpdatesCalculation(slug, updates, returning = queries.pure[IO])

    queries foreach givenRunningQuerySucceeds

    updater.updateProject(slug, updates).asserting(_ shouldBe ProjectUpdater.Result.Updated)
  }

  private val projectExistenceChecker = mock[ProjectExistenceChecker[IO]]
  private val updateQueriesCalculator = mock[UpdateQueriesCalculator[IO]]
  private val tsClient                = mock[TSClient[IO]]
  private val tsWriteLock: TsWriteLock[IO] = Lock.none[IO, projects.Slug]
  private lazy val updater =
    new ProjectUpdaterImpl[IO](projectExistenceChecker, updateQueriesCalculator, tsClient, tsWriteLock)

  private def givenProjectExistenceChecking(slug: projects.Slug, returning: IO[Boolean]) =
    (projectExistenceChecker.checkExists _)
      .expects(slug)
      .returning(returning)

  private def givenUpdatesCalculation(slug: projects.Slug, updates: ProjectUpdates, returning: IO[List[SparqlQuery]]) =
    (updateQueriesCalculator.calculateUpdateQueries _)
      .expects(slug, updates)
      .returning(returning)

  private def givenRunningQuerySucceeds(query: SparqlQuery) =
    (tsClient.updateWithNoResult _)
      .expects(query)
      .returning(().pure[IO])
}
