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

package io.renku.knowledgegraph.users.projects.finder

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.persons
import io.renku.http.client.GitLabClient
import io.renku.knowledgegraph.users.projects.Endpoint.Criteria
import io.renku.knowledgegraph.users.projects.model.Project
import org.typelevel.log4cats.Logger

private trait GLCreatorsNamesAdder[F[_]] {
  def addCreatorsNames(criteria: Criteria)(projects: List[Project]): F[List[Project]]
}

private object GLCreatorsNamesAdder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GLCreatorsNamesAdder[F]] =
    GLCreatorFinder[F].map(new GLCreatorsNamesAdderImpl[F](_))
}

private class GLCreatorsNamesAdderImpl[F[_]: MonadThrow](creatorFinder: GLCreatorFinder[F])
    extends GLCreatorsNamesAdder[F] {

  override def addCreatorsNames(criteria: Criteria)(projects: List[Project]): F[List[Project]] =
    findDistinctCreatorIds(projects)
      .map(fetchCreatorName(criteria))
      .sequence
      .map(updateProjects(projects))

  private val findDistinctCreatorIds: List[Project] => List[persons.GitLabId] =
    _.flatMap {
      case _: Project.Activated    => None
      case p: Project.NotActivated => p.maybeCreatorId
    }.distinct

  private type CreatorInfo = (persons.GitLabId, Option[persons.Name])

  private def fetchCreatorName(criteria: Criteria)(id: persons.GitLabId): F[CreatorInfo] =
    creatorFinder.findCreatorName(id)(criteria.maybeUser.map(_.accessToken)).map(id -> _)

  private def updateProjects(projects: List[Project]): List[CreatorInfo] => List[Project] = creators =>
    projects.map {
      case proj @ Project.NotActivated(_, _, _, _, _, Some(creatorId), _, _, _) =>
        creators
          .find(_._1 == creatorId)
          .map { case (_, maybeName) => proj.copy(maybeCreator = maybeName) }
          .getOrElse(proj)
      case proj => proj
    }
}
