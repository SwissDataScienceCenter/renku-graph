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

package io.renku.knowledgegraph.projects.create

import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.core.client.{ProjectRepository, UserInfo, NewProject => CoreNewProject}
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.http.client.{GitLabClient, UserAccessToken}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.gitlab.UserInfoFinder

private trait CorePayloadFinder[F[_]] {
  def findCorePayload(newProject: NewProject, authUser: AuthUser): F[CoreNewProject]
}

private object CorePayloadFinder {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient](config: Config): F[CorePayloadFinder[F]] =
    GitLabUrlLoader[F](config).map(
      new CorePayloadFinderImpl[F](NamespaceFinder[F], UserInfoFinder[F], _)
    )
}

private class CorePayloadFinderImpl[F[_]: Async: NonEmptyParallel](namespaceFinder: NamespaceFinder[F],
                                                                   userInfoFinder: UserInfoFinder[F],
                                                                   gitLabUrl:      GitLabUrl
) extends CorePayloadFinder[F] {

  override def findCorePayload(newProject: NewProject, authUser: AuthUser): F[CoreNewProject] =
    (findNamespace(newProject, authUser.accessToken), findUserInfo(authUser))
      .parMapN(toPayload(newProject))

  private def findNamespace(newProject: NewProject, accessToken: UserAccessToken): F[Namespace.WithName] =
    namespaceFinder
      .findNamespace(newProject.namespace.identifier, accessToken)
      .adaptError(CreationFailures.onFindingNamespace(newProject.namespace.identifier, _))
      .flatMap {
        case Some(ns) => ns.pure[F]
        case None     => CreationFailures.noNamespaceFound(newProject.namespace).raiseError[F, Namespace.WithName]
      }

  private def findUserInfo(authUser: AuthUser): F[UserInfo] =
    userInfoFinder
      .findUserInfo(authUser.accessToken)
      .adaptError(CreationFailures.onFindingUserInfo(authUser.id, _))
      .flatMap {
        case Some(info) => info.pure[F]
        case None       => CreationFailures.noUserInfoFound(authUser.id).raiseError[F, UserInfo]
      }

  private def toPayload(newProject: NewProject): (Namespace.WithName, UserInfo) => CoreNewProject = {
    case (namespace, userInfo) =>
      CoreNewProject(
        ProjectRepository.of(gitLabUrl),
        namespace.name,
        newProject.slug.toPath.asName,
        newProject.template,
        newProject.branch,
        maybeDescription = newProject.maybeDescription,
        userInfo
      )
  }
}
