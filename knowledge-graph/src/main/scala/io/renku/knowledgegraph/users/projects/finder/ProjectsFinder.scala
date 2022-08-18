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

import Endpoint.Criteria.Filters._
import Endpoint._
import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.http.client.GitLabClient
import io.renku.http.rest.paging.PagingResponse
import io.renku.triplesstore.SparqlQueryTimeRecorder
import model._
import org.typelevel.log4cats.Logger

private[projects] trait ProjectsFinder[F[_]] {
  def findProjects(criteria: Criteria): F[PagingResponse[Project]]
}

private[projects] object ProjectsFinder {
  def apply[F[_]: Async: GitLabClient: Logger: SparqlQueryTimeRecorder]: F[ProjectsFinder[F]] =
    (TSProjectFinder[F], GLProjectFinder[F]).mapN(new ProjectsFinderImpl(_, _))
}

private class ProjectsFinderImpl[F[_]: MonadThrow](
    tsProjectsFinder: TSProjectFinder[F],
    glProjectsFinder: GLProjectFinder[F]
) extends ProjectsFinder[F] {

  import glProjectsFinder._
  import tsProjectsFinder._

  override def findProjects(criteria: Criteria): F[PagingResponse[model.Project]] =
    (findProjectsInTS(criteria) -> findProjectsInGL(criteria))
      .mapN(mergeFavouringActivated)
      .map(filterBy(criteria.filters.state))
      .map(_.sortBy(_.name))
      .flatMap(PagingResponse.from[F, model.Project](_, criteria.paging))

  private val mergeFavouringActivated
      : (List[model.Project.Activated], List[model.Project.NotActivated]) => List[model.Project] = {
    (tsFound, glFound) =>
      glFound.foldLeft(tsFound.asInstanceOf[List[model.Project]]) {
        case (all, glProj) if all.exists(_.path == glProj.path) => all
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
