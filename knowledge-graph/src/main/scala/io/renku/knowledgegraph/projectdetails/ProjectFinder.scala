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

package io.renku.knowledgegraph.projectdetails

import GitLabProjectFinder.GitLabProject
import KGProjectFinder.{KGParent, KGProject}
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.graph.model.projects.Path
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import model._
import org.typelevel.log4cats.Logger

private trait ProjectFinder[F[_]] {
  def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[Project]]
}

private class ProjectFinderImpl[F[_]: MonadThrow: Parallel: AccessTokenFinder](
    kgProjectFinder:     KGProjectFinder[F],
    gitLabProjectFinder: GitLabProjectFinder[F]
) extends ProjectFinder[F] {

  private val accessTokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import accessTokenFinder._
  import gitLabProjectFinder.{findProject => findProjectInGitLab}
  import kgProjectFinder.{findProject => findInKG}

  def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[Project]] =
    ((OptionT(findInKG(path, maybeAuthUser)), findInGitLab(path)) parMapN (merge(path, _, _))).value

  private def findInGitLab(path: Path) =
    OptionT(findAccessToken(path)) >>= { implicit accessToken => OptionT(findProjectInGitLab(path)) }

  private def merge(path: Path, kgProject: KGProject, gitLabProject: GitLabProject) = Project(
    resourceId = kgProject.resourceId,
    id = gitLabProject.id,
    path = path,
    name = kgProject.name,
    maybeDescription = kgProject.maybeDescription,
    visibility = kgProject.visibility,
    created = Creation(
      date = kgProject.created.date,
      kgProject.created.maybeCreator.map(creator => Creator(creator.resourceId, creator.maybeEmail, creator.name))
    ),
    updatedAt = gitLabProject.updatedAt,
    urls = gitLabProject.urls,
    forking = Forking(gitLabProject.forksCount, kgProject.maybeParent.toParentProject),
    keywords = kgProject.keywords,
    starsCount = gitLabProject.starsCount,
    permissions = gitLabProject.permissions,
    statistics = gitLabProject.statistics,
    maybeVersion = kgProject.maybeVersion
  )

  private implicit class ParentOps(maybeParent: Option[KGParent]) {
    lazy val toParentProject: Option[ParentProject] =
      maybeParent.map { case KGParent(resourceId, path, name, created) =>
        ParentProject(
          resourceId,
          path,
          name,
          Creation(created.date,
                   created.maybeCreator.map(creator => Creator(creator.resourceId, creator.maybeEmail, creator.name))
          )
        )
      }
  }
}

private object ProjectFinder {

  import io.renku.triplesstore.SparqlQueryTimeRecorder

  def apply[F[_]: Async: Parallel: GitLabClient: AccessTokenFinder: Logger: SparqlQueryTimeRecorder]
      : F[ProjectFinder[F]] = for {
    kgProjectFinder     <- KGProjectFinder[F]
    gitLabProjectFinder <- GitLabProjectFinder[F]
  } yield new ProjectFinderImpl(kgProjectFinder, gitLabProjectFinder)
}
