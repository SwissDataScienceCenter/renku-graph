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

package io.renku.knowledgegraph.projects.update

import ProvisioningStatusFinder.ProvisioningStatus.Unhealthy
import cats.effect.Async
import cats.syntax.all._
import cats.{Applicative, NonEmptyParallel}
import io.renku.core.client.{Branch, RenkuCoreClient, RenkuCoreUri, UserInfo, ProjectUpdates => CoreProjectUpdates}
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.gitlab.UserInfoFinder
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, ProjectUpdates => TGProjectUpdates}
import org.typelevel.log4cats.Logger

private trait ProjectUpdater[F[_]] {
  def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Unit]
}

private object ProjectUpdater {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: MetricsRegistry: Logger]: F[ProjectUpdater[F]] =
    (ProvisioningStatusFinder[F], TriplesGeneratorClient[F], RenkuCoreClient[F]())
      .mapN(
        new ProjectUpdaterImpl[F](_,
                                  BranchProtectionCheck[F],
                                  ProjectGitUrlFinder[F],
                                  UserInfoFinder[F],
                                  GLProjectUpdater[F],
                                  _,
                                  _,
                                  TGUpdatesFinder[F]
        )
      )
}

private class ProjectUpdaterImpl[F[_]: Async: NonEmptyParallel: Logger](
    provisioningStatusFinder: ProvisioningStatusFinder[F],
    branchProtectionCheck:    BranchProtectionCheck[F],
    projectGitUrlFinder:      ProjectGitUrlFinder[F],
    userInfoFinder:           UserInfoFinder[F],
    glProjectUpdater:         GLProjectUpdater[F],
    tgClient:                 TriplesGeneratorClient[F],
    renkuCoreClient:          RenkuCoreClient[F],
    tgUpdatesFinder:          TGUpdatesFinder[F]
) extends ProjectUpdater[F] {

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Unit] =
    if (updates.onlyGLUpdateNeeded)
      updateGL(slug, updates, authUser)
        .flatMap(findTGUpdates(slug, updates, _))
        .flatMap(updateTG(slug, _))
    else
      checkPrerequisites(slug, authUser) >>= { defaultBranchInfo =>
        for {
          coreUpdates    <- findCoreProjectUpdates(slug, updates, authUser)
          coreUri        <- findCoreUri(coreUpdates, authUser)
          corePushBranch <- updateCore(slug, coreUri, coreUpdates, authUser)
          glUpdated      <- updateGL(slug, updates, authUser)
          tgUpdates      <- findTGUpdates(slug, updates, glUpdated, defaultBranchInfo, corePushBranch)
          _              <- updateTG(slug, tgUpdates)
          _              <- failIfPRNeeded(tgUpdates, defaultBranchInfo, corePushBranch)
        } yield ()
      }

  private def updateGL(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Option[GLUpdatedProject]] =
    glProjectUpdater
      .updateProject(slug, updates, authUser.accessToken)
      .adaptError(UpdateFailures.onGLUpdate(slug, _))
      .flatMap(_.fold(_.raiseError[F, Option[GLUpdatedProject]], _.pure[F]))

  private def findTGUpdates(slug:                  projects.Slug,
                            updates:               ProjectUpdates,
                            maybeGLUpdatedProject: Option[GLUpdatedProject]
  ) = tgUpdatesFinder
    .findTGProjectUpdates(updates, maybeGLUpdatedProject)
    .adaptError(UpdateFailures.onTGUpdatesFinding(slug, _))

  private def findTGUpdates(slug:                  projects.Slug,
                            updates:               ProjectUpdates,
                            maybeGLUpdatedProject: Option[GLUpdatedProject],
                            maybeDefaultBranch:    Option[DefaultBranch],
                            corePushBranch:        Branch
  ) = tgUpdatesFinder
    .findTGProjectUpdates(updates, maybeGLUpdatedProject, maybeDefaultBranch, corePushBranch)
    .adaptError(UpdateFailures.onTGUpdatesFinding(slug, _))

  private def updateTG(slug: projects.Slug, updates: TGProjectUpdates): F[Unit] =
    tgClient
      .updateProject(slug, updates)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(UpdateFailures.onTSUpdate(slug, _).raiseError[F, Unit], _.pure[F]))

  private def checkPrerequisites(slug: projects.Slug, authUser: AuthUser): F[Option[DefaultBranch]] =
    (noProvisioningFailures(slug), findDefaultBranchInfo(slug, authUser))
      .parMapN((_, branchInfo) => branchInfo)

  private def noProvisioningFailures(slug: projects.Slug): F[Unit] =
    provisioningStatusFinder
      .checkHealthy(slug)
      .adaptError(UpdateFailures.onProvisioningStatusCheck(slug, _))
      .flatMap {
        case r @ Unhealthy(_) => UpdateFailures.onProvisioningNotHealthy(slug, r).raiseError[F, Unit]
        case _                => ().pure[F]
      }

  private def findDefaultBranchInfo(slug: projects.Slug, authUser: AuthUser): F[Option[DefaultBranch]] =
    branchProtectionCheck
      .findDefaultBranchInfo(slug, authUser.accessToken)
      .adaptError(UpdateFailures.onBranchAccessCheck(slug, authUser.id, _))

  private def findCoreProjectUpdates(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser) =
    (findProjectGitUrl(slug, authUser) -> findUserInfo(authUser))
      .parMapN(CoreProjectUpdates(_, _, updates.newDescription, updates.newKeywords))

  private def findProjectGitUrl(slug: projects.Slug, authUser: AuthUser): F[projects.GitHttpUrl] =
    projectGitUrlFinder
      .findGitUrl(slug, authUser.accessToken)
      .adaptError(UpdateFailures.onFindingProjectGitUrl(slug, _))
      .flatMap {
        case Some(url) => url.pure[F]
        case None      => UpdateFailures.cannotFindProjectGitUrl.raiseError[F, projects.GitHttpUrl]
      }

  private def findUserInfo(authUser: AuthUser): F[UserInfo] =
    userInfoFinder
      .findUserInfo(authUser.accessToken)
      .adaptError(UpdateFailures.onFindingUserInfo(authUser.id, _))
      .flatMap {
        case Some(info) => info.pure[F]
        case None       => UpdateFailures.cannotFindUserInfo(authUser.id).raiseError[F, UserInfo]
      }

  private def findCoreUri(updates: CoreProjectUpdates, authUser: AuthUser): F[RenkuCoreUri.Versioned] =
    renkuCoreClient
      .findCoreUri(updates.projectUrl, updates.userInfo, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(UpdateFailures.onFindingCoreUri(_).raiseError[F, RenkuCoreUri.Versioned], _.pure[F]))

  private def updateCore(slug:     projects.Slug,
                         coreUri:  RenkuCoreUri.Versioned,
                         updates:  CoreProjectUpdates,
                         authUser: AuthUser
  ): F[Branch] =
    renkuCoreClient
      .updateProject(coreUri, updates, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(UpdateFailures.onCoreUpdate(slug, _).raiseError[F, Branch], _.pure[F]))

  private def failIfPRNeeded(tgUpdates:         TGProjectUpdates,
                             defaultBranchInfo: Option[DefaultBranch],
                             pushBranch:        Branch
  ): F[Unit] =
    Applicative[F].unlessA(defaultBranchInfo.map(_.branch) contains pushBranch) {
      UpdateFailures.corePushedToNonDefaultBranch(tgUpdates, defaultBranchInfo, pushBranch).raiseError[F, Unit]
    }
}
