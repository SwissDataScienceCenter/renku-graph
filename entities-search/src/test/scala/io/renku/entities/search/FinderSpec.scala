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

package io.renku.entities.search

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.entities.search.EntityConverters._
import io.renku.http.client.GitLabGenerators.userAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities.{Entity => _, _}
import io.renku.http.rest.SortBy
import io.renku.http.rest.paging.PagingResponse
import io.renku.http.server.security.model.AuthUser
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.tinytypes.StringTinyType
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TestProjectsDataset}
import org.scalatest.Suite

import java.time.Instant

trait FinderSpec {
  self: Suite with TestProjectsDataset with AsyncIOSpec =>

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()

  protected[search] def entitiesFinder(implicit pcc: ProjectsConnectionConfig): EntitiesFinder[IO] = {
    implicit val sqtr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new EntitiesFinderImpl[IO](pcc, EntitiesFinder.finders)
  }

  protected[search] implicit class PagingResponseOps(response: PagingResponse[model.Entity]) {
    lazy val resultsWithSkippedMatchingScore: List[model.Entity] = response.results.skipMatchingScore
  }

  protected[search] implicit class ResultsOps(results: List[model.Entity]) {
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

  protected[search] implicit class EntityOps(entity: model.Entity) {
    lazy val dateAsInstant: Instant = entity match {
      case proj:     model.Entity.Project  => proj.dateModified.value
      case ds:       model.Entity.Dataset  => ds.dateModified.map(_.value).getOrElse(ds.date.instant)
      case workflow: model.Entity.Workflow => workflow.date.value
      case person:   model.Entity.Person   => person.date.value
    }
  }

  protected[search] def allEntitiesFrom(project: RenkuProject): List[model.Entity] =
    List.empty[model.Entity].addAllEntitiesFrom(project)

  protected[search] implicit class EntitiesOps(entities: List[model.Entity]) {

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
      project.plans.map(_ -> project).map(_.to[model.Entity.Workflow]) ::: entities
    }.distinct

    def addAllPersonsFrom(project: RenkuProject): List[model.Entity] =
      addPersons((project.members.map(_.person) ++ project.maybeCreator).toList)
        .addPersons(project.datasets.flatMap(_.provenance.creators.toList))
        .addPersons(project.activities.map(_.author))
        .addPersons(project.plans.flatMap(_.creators))
        .distinct

    def removeAllPersons(): List[model.Entity] = entities.filterNot {
      case _: model.Entity.Person => true
      case _ => false
    }

    def addPersons(persons: List[Person]): List[model.Entity] = {
      persons.map(_.to[model.Entity.Person]) ::: entities
    }.distinct
  }

  implicit class PersonOps(person: Person) {
    lazy val toAuthUser: AuthUser = AuthUser(person.maybeGitLabId.get, userAccessTokens.generateOne)
  }

  protected[search] def nameOrdering[TT <: StringTinyType]: Ordering[TT] = Ordering.by(_.value.toLowerCase)
}
