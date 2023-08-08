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

package io.renku.entities.viewings.search

import cats.effect.IO
import cats.syntax.all._
import com.softwaremill.diffx.Diff
import io.circe.Decoder
import io.renku.entities.search.FinderSpecOps
import io.renku.entities.search.diff.SearchDiffInstances
import io.renku.entities.search.model.{Entity => SearchEntity}
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.entities.viewings.EntityViewings
import io.renku.entities.viewings.search.RecentEntitiesFinder.EntityType
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.{Person => TestPerson}
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.graph.model.{datasets, projects}
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent, UserId}
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.Instant

abstract class SearchTestBase
    extends AnyFlatSpec
    with should.Matchers
    with EntitiesGenerators
    with FinderSpecOps
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets
    with AdditionalMatchers
    with SearchDiffInstances
    with EntityViewings
    with IOSpec {

  implicit val queryTimeRecorder: SparqlQueryTimeRecorder[IO] =
    TestSparqlQueryTimeRecorder[IO].unsafeRunSync()

  lazy val tsClient = tsClientIO.unsafeRunSync()

  val token: UserAccessToken = AccessToken.PersonalAccessToken("nonblank")

  def storeProjectViewed(userId: GitLabId, dateViewed: Instant, slug: projects.Slug): Unit =
    provision(ProjectViewedEvent(slug, dateViewed, UserId.GLId(userId).some)).unsafeRunSync()

  def storeDatasetViewed(userId: GitLabId, dateViewed: Instant, ident: datasets.Identifier): Unit =
    provision(DatasetViewedEvent(ident, dateViewed, userId.some)).unsafeRunSync()

  def personGen: Gen[TestPerson] =
    personEntities(maybeGitLabIds = personGitLabIds.map(Some(_)))

  def printSparqlQuery(q: SparqlQuery) =
    println(q.toString.split('\n').zipWithIndex.map(t => f"${t._2}%2d ${t._1}").mkString("\n"))

  def projectDecoder: Decoder[List[SearchEntity.Project]] =
    ResultsDecoder[List, SearchEntity.Project](Variables.Project.decoder)

  def datasetDecoder: Decoder[List[SearchEntity.Dataset]] =
    ResultsDecoder[List, SearchEntity.Dataset](Variables.Dataset.decoder)

  implicit val searchCriteriaEntityTypeDiff: Diff[EntityType] =
    Diff.diffForString.contramap(_.name)
}
