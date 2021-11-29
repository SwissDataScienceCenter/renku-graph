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

package io.renku.knowledgegraph.projects.rest

import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects.Path
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder._
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.projects.model._
import io.renku.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import io.renku.knowledgegraph.projects.rest.KGProjectFinder.{KGProject, Parent}
import org.typelevel.log4cats.Logger

import scala.util.Try

private trait ProjectFinder[F[_]] {
  def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[Project]]
}

private class ProjectFinderImpl[F[_]: MonadThrow: Parallel](
    kgProjectFinder:     KGProjectFinder[F],
    gitLabProjectFinder: GitLabProjectFinder[F],
    accessTokenFinder:   AccessTokenFinder[F]
) extends ProjectFinder[F] {

  import accessTokenFinder._
  import gitLabProjectFinder.{findProject => findProjectInGitLab}
  import kgProjectFinder.{findProject => findInKG}

  def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[Project]] =
    ((OptionT(findInKG(path, maybeAuthUser)), findInGitLab(path, maybeAuthUser)) parMapN (merge(path, _, _))).value

  private def findInGitLab(path: Path, maybeAuthUser: Option[AuthUser]) = for {
    accessToken   <- OptionT.fromOption[F](maybeAuthUser.map(_.accessToken)) orElseF findAccessToken(path)
    gitLabProject <- findProjectInGitLab(path, Some(accessToken))
  } yield gitLabProject

  private def merge(path: Path, kgProject: KGProject, gitLabProject: GitLabProject) = Project(
    id = gitLabProject.id,
    path = path,
    name = kgProject.name,
    maybeDescription = kgProject.maybeDescription,
    visibility = kgProject.visibility,
    created = Creation(
      date = kgProject.created.date,
      maybeCreator = kgProject.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name))
    ),
    updatedAt = gitLabProject.updatedAt,
    urls = gitLabProject.urls,
    forking = Forking(gitLabProject.forksCount, kgProject.maybeParent.toParentProject),
    keywords = kgProject.keywords,
    starsCount = gitLabProject.starsCount,
    permissions = gitLabProject.permissions,
    statistics = gitLabProject.statistics,
    version = kgProject.version
  )

  private implicit class ParentOps(maybeParent: Option[Parent]) {
    lazy val toParentProject: Option[ParentProject] =
      (maybeParent -> maybeParent.flatMap(_.resourceId.as[Try, Path].toOption)) mapN { case (parent, path) =>
        ParentProject(
          path,
          parent.name,
          Creation(parent.created.date,
                   parent.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name))
          )
        )
      }
  }
}

private object ProjectFinder {

  import io.renku.rdfstore.SparqlQueryTimeRecorder

  def apply[F[_]: Async: Parallel: Logger](
      gitLabThrottler: Throttler[F, GitLab],
      timeRecorder:    SparqlQueryTimeRecorder[F]
  ): F[ProjectFinder[F]] = for {
    kgProjectFinder     <- KGProjectFinder[F](timeRecorder)
    gitLabProjectFinder <- GitLabProjectFinder[F](gitLabThrottler)
    accessTokenFinder   <- AccessTokenFinder[F]
  } yield new ProjectFinderImpl(kgProjectFinder, gitLabProjectFinder, accessTokenFinder)
}
