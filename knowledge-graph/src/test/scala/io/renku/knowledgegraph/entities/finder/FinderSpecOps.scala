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

package io.renku.knowledgegraph.entities
package finder

import cats.effect.IO
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.{Entity => _, _}
import io.renku.http.rest.SortBy
import io.renku.http.rest.paging.PagingResponse
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryJenaForSpec, RenkuDataset, SparqlQueryTimeRecorder}
import org.scalatest.TestSuite

import java.time.Instant

trait FinderSpecOps {
  self: TestSuite with InMemoryJenaForSpec with RenkuDataset =>

  protected[finder] trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val finder = new EntitiesFinderImpl[IO](renkuDSConnectionInfo)
  }

  protected implicit class PagingResponseOps(response: PagingResponse[model.Entity]) {
    lazy val resultsWithSkippedMatchingScore: List[model.Entity] = response.results.skipMatchingScore
  }

  protected implicit class ResultsOps(results: List[model.Entity]) {
    lazy val skipMatchingScore: List[model.Entity] = results.map {
      case proj:     model.Entity.Project  => proj.copy(matchingScore = model.MatchingScore.min)
      case ds:       model.Entity.Dataset  => ds.copy(matchingScore = model.MatchingScore.min)
      case workflow: model.Entity.Workflow => workflow.copy(matchingScore = model.MatchingScore.min)
      case person:   model.Entity.Person   => person.copy(matchingScore = model.MatchingScore.min)
    }

    lazy val use: SortBy.Direction => List[model.Entity] = {
      case SortBy.Direction.Asc  => results
      case SortBy.Direction.Desc => results.reverse
    }
  }

  protected implicit class EntityOps(entity: model.Entity) {
    lazy val dateAsInstant: Instant = entity match {
      case proj:     model.Entity.Project  => proj.date.value
      case ds:       model.Entity.Dataset  => ds.date.instant
      case workflow: model.Entity.Workflow => workflow.date.value
      case person:   model.Entity.Person   => person.date.value
    }
  }

  protected def allEntitiesFrom(project: RenkuProject): List[model.Entity] =
    List.empty[model.Entity].addAllEntitiesFrom(project)

  protected implicit class EntitiesOps(entities: List[model.Entity]) {

    def addAllEntitiesFrom(project: RenkuProject): List[model.Entity] = {
      List(project.to[model.Entity.Project])
        .addAllDatasetsFrom(project)
        .addAllPlansFrom(project)
        .addAllPersonsFrom(project) ::: entities
    }.distinct

    def addAllDatasetsFrom(project: RenkuProject): List[model.Entity] = {
      project.datasets.map(_ -> project).map(_.to[model.Entity.Dataset]) ::: entities
    }.distinct

    def addAllPlansFrom(project: RenkuProject): List[model.Entity] = {
      project.plans.toList.map(_ -> project).map(_.to[model.Entity.Workflow]) ::: entities
    }.distinct

    def addAllPersonsFrom(project: RenkuProject): List[model.Entity] =
      addPersons((project.members ++ project.maybeCreator).toList)
        .addPersons(project.datasets.flatMap(_.provenance.creators.toList))
        .addPersons(project.activities.map(_.author))
        .distinct

    def addPersons(persons: List[Person]): List[model.Entity] = {
      persons.map(_.to[model.Entity.Person]) ::: entities
    }.distinct
  }

  implicit class PersonOps(person: Person) {
    lazy val toAuthUser: AuthUser = AuthUser(person.maybeGitLabId.get, accessTokens.generateOne)
  }
}
