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

import Endpoint.Criteria.Filters._
import Endpoint._
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.rest.paging.PagingResponse
import io.renku.triplesstore.SparqlQueryTimeRecorder
import model._
import org.typelevel.log4cats.Logger

private[projects] trait ProjectsFinder[F[_]] {
  def findProjects(criteria: Criteria): F[PagingResponse[Project]]
}

private[projects] object ProjectsFinder {
  def apply[F[_]: Async: Parallel: GitLabClient: Logger: SparqlQueryTimeRecorder]: F[ProjectsFinder[F]] =
    (TSProjectFinder[F], GLProjectFinder[F], GLCreatorsNamesAdder[F]).mapN(new ProjectsFinderImpl(_, _, _))

  implicit val nameOrdering: Ordering[projects.Name] = Ordering.by(_.value.toLowerCase)
}

private class ProjectsFinderImpl[F[_]: MonadThrow: NonEmptyParallel](
    tsProjectsFinder:     TSProjectFinder[F],
    glProjectsFinder:     GLProjectFinder[F],
    glCreatorsNamesAdder: GLCreatorsNamesAdder[F]
) extends ProjectsFinder[F] {

  import ProjectsFinder.nameOrdering
  import glCreatorsNamesAdder._
  import glProjectsFinder._
  import tsProjectsFinder._

  override def findProjects(criteria: Criteria): F[PagingResponse[model.Project]] =
    queryProjects(criteria)
      .map(filterBy(criteria.filters.state))
      .map(_.sortBy(_.name))
      .flatMap(PagingResponse.from[F, model.Project](_, criteria.paging))
      .flatMap(_.flatMapResults(addCreatorsNames(criteria)))

  private def queryProjects(criteria: Criteria): F[List[Project]] =
    criteria.filters.state match {
      case ActivationState.Activated => findProjectsInTS(criteria).widen
      case _ => (findProjectsInTS(criteria) -> findProjectsInGL(criteria)).parMapN(mergeFavouringActivated)
    }

  private val mergeFavouringActivated
      : (List[model.Project.Activated], List[model.Project.NotActivated]) => List[model.Project] = {
    (tsFound, glFound) =>
      glFound.foldLeft(tsFound.asInstanceOf[List[model.Project]]) {
        case (all, glProj) if all.exists(_.slug == glProj.slug) => all
        case (all, glProj)                                      => glProj :: all
      }
  }

  private def filterBy(activationState: ActivationState): List[model.Project] => List[model.Project] = projects =>
    if (activationState == ActivationState.All) projects
    else
      projects.filter {
        case _: model.Project.Activated    => activationState == ActivationState.Activated
        case _: model.Project.NotActivated => activationState == ActivationState.NotActivated
      }
}
