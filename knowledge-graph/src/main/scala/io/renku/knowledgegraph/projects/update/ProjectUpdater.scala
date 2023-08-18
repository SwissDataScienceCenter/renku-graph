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

package io.renku.knowledgegraph.projects.update

import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.core.client.{RenkuCoreClient, RenkuCoreUri, UserInfo, ProjectUpdates => CoreProjectUpdates}
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, ProjectUpdates => TGProjectUpdates}
import org.typelevel.log4cats.Logger

private trait ProjectUpdater[F[_]] {
  def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Unit]
}

private object ProjectUpdater {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: MetricsRegistry: Logger]: F[ProjectUpdater[F]] =
    (TriplesGeneratorClient[F], RenkuCoreClient[F]())
      .mapN(
        new ProjectUpdaterImpl[F](BranchProtectionCheck[F],
                                  ProjectGitUrlFinder[F],
                                  UserInfoFinder[F],
                                  GLProjectUpdater[F],
                                  _,
                                  _
        )
      )
}

private class ProjectUpdaterImpl[F[_]: Async: NonEmptyParallel: Logger](branchProtectionCheck: BranchProtectionCheck[F],
                                                                        projectGitUrlFinder: ProjectGitUrlFinder[F],
                                                                        userInfoFinder:      UserInfoFinder[F],
                                                                        glProjectUpdater:    GLProjectUpdater[F],
                                                                        tgClient:            TriplesGeneratorClient[F],
                                                                        renkuCoreClient:     RenkuCoreClient[F]
) extends ProjectUpdater[F] {

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Unit] =
    if (updates.onlyGLUpdateNeeded)
      updateGL(slug, updates, authUser) >> updateTG(slug, updates)
    else
      canPushToDefaultBranch(slug, authUser) >> {
        for {
          coreUpdates <- findCoreProjectUpdates(slug, updates, authUser)
          coreUri     <- findCoreUri(coreUpdates, authUser)
          _           <- updateCore(coreUri, coreUpdates, authUser)
          _           <- updateGL(slug, updates, authUser)
          _           <- updateTG(slug, updates)
        } yield ()
      }

  private def updateGL(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Unit] =
    glProjectUpdater
      .updateProject(slug, updates, authUser.accessToken)
      .adaptError(Failure.onGLUpdate(slug, _))
      .flatMap(_.fold(errMsg => Failure.badRequestOnGLUpdate(errMsg).raiseError[F, Unit], _.pure[F]))

  private def updateTG(slug: projects.Slug, updates: ProjectUpdates): F[Unit] =
    tgClient
      .updateProject(
        slug,
        TGProjectUpdates(newDescription = updates.newDescription,
                         newImages = updates.newImage.map(_.toList),
                         newKeywords = updates.newKeywords,
                         newVisibility = updates.newVisibility
        )
      )
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(Failure.onTSUpdate(slug, _).raiseError[F, Unit], _.pure[F]))

  private def canPushToDefaultBranch(slug: projects.Slug, authUser: AuthUser): F[Unit] =
    branchProtectionCheck
      .canPushToDefaultBranch(slug, authUser.accessToken)
      .adaptError(Failure.onBranchAccessCheck(slug, authUser.id, _))
      .flatMap {
        case false => Failure.cannotPushToBranch.raiseError[F, Unit]
        case true  => ().pure[F]
      }

  private def findCoreProjectUpdates(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser) =
    (findProjectGitUrl(slug, authUser) -> findUserInfo(authUser))
      .parMapN(CoreProjectUpdates(_, _, updates.newDescription, updates.newKeywords))

  private def findProjectGitUrl(slug: projects.Slug, authUser: AuthUser): F[projects.GitHttpUrl] =
    projectGitUrlFinder
      .findGitUrl(slug, authUser.accessToken)
      .adaptError(Failure.onFindingProjectGitUrl(slug, _))
      .flatMap {
        case Some(url) => url.pure[F]
        case None      => Failure.cannotFindProjectGitUrl.raiseError[F, projects.GitHttpUrl]
      }

  private def findUserInfo(authUser: AuthUser): F[UserInfo] =
    userInfoFinder
      .findUserInfo(authUser.accessToken)
      .adaptError(Failure.onFindingUserInfo(authUser.id, _))
      .flatMap {
        case Some(info) => info.pure[F]
        case None       => Failure.cannotFindUserInfo(authUser.id).raiseError[F, UserInfo]
      }

  private def findCoreUri(updates: CoreProjectUpdates, authUser: AuthUser): F[RenkuCoreUri.Versioned] =
    renkuCoreClient
      .findCoreUri(updates.projectUrl, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(Failure.onFindingCoreUri(_).raiseError[F, RenkuCoreUri.Versioned], _.pure[F]))

  private def updateCore(coreUri: RenkuCoreUri.Versioned, updates: CoreProjectUpdates, authUser: AuthUser): F[Unit] =
    renkuCoreClient
      .updateProject(coreUri, updates, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(Failure.onCoreUpdate(_).raiseError[F, Unit], _.pure[F]))
}
