/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.entities.Project.GitLabProjectInfo
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[triplesgenerated] trait ProjectInfoFinder[F[_]] {
  def findProjectInfo(path: projects.Path)(implicit
      maybeAccessToken:     Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private[triplesgenerated] object ProjectInfoFinder {
  def apply[F[_]: Async: NonEmptyParallel: Logger](gitLabThrottler: Throttler[F, GitLab]): F[ProjectInfoFinder[F]] =
    for {
      projectFinder <- ProjectFinder[F](gitLabThrottler)
      membersFinder <- ProjectMembersFinder[F](gitLabThrottler)
    } yield new ProjectInfoFinderImpl(projectFinder, membersFinder)
}

private[triplesgenerated] class ProjectInfoFinderImpl[F[_]: MonadThrow: Logger](
    projectFinder: ProjectFinder[F],
    membersFinder: ProjectMembersFinder[F]
) extends ProjectInfoFinder[F] {

  import membersFinder._
  import projectFinder._

  override def findProjectInfo(path: projects.Path)(implicit
      maybeAccessToken:              Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]] =
    findProject(path) >>= {
      case None          => EitherT.rightT[F, ProcessingRecoverableError](Option.empty[GitLabProjectInfo])
      case Some(project) => findProjectMembers(path).map(members => project.copy(members = members).some)
    }
}
