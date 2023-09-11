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

import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import com.typesafe.config.Config
import io.renku.core.client.{RenkuCoreClient, NewProject => CorePayload}
import io.renku.http.client.{GitLabClient, UserAccessToken}
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.api.{HookCreationResult, WebhookServiceClient}
import org.typelevel.log4cats.Logger

private trait ProjectCreator[F[_]] {
  def createProject(newProject: NewProject, authUser: AuthUser): F[Unit]
}

private object ProjectCreator {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: Logger: MetricsRegistry](
      config: Config
  ): F[ProjectCreator[F]] =
    (CorePayloadFinder[F](config), RenkuCoreClient[F](config), WebhookServiceClient[F](config))
      .mapN(new ProjectCreatorImpl[F](GLProjectCreator[F], _, _, _))
}

private class ProjectCreatorImpl[F[_]: MonadThrow](
    glProjectCreator:  GLProjectCreator[F],
    corePayloadFinder: CorePayloadFinder[F],
    coreClient:        RenkuCoreClient[F],
    wsClient:          WebhookServiceClient[F]
) extends ProjectCreator[F] {

  override def createProject(newProject: NewProject, authUser: AuthUser): F[Unit] =
    for {
      corePayload <- findCorePayload(newProject, authUser)
      glCreated   <- createProjectInGL(newProject, authUser.accessToken)
      _           <- createProjectInCore(newProject, corePayload, authUser)
      _           <- activateProject(newProject, glCreated, authUser.accessToken)
    } yield ()

  private def findCorePayload(newProject: NewProject, authUser: AuthUser): F[CorePayload] =
    corePayloadFinder
      .findCorePayload(newProject, authUser)

  private def createProjectInGL(newProject: NewProject, accessToken: UserAccessToken): F[GLCreatedProject] =
    glProjectCreator
      .createProject(newProject, accessToken)
      .adaptError(CreateFailures.onGLCreation(newProject.slug, _))
      .flatMap(_.fold(_.raiseError[F, GLCreatedProject], _.pure[F]))

  private def createProjectInCore(newProject: NewProject, corePayload: CorePayload, authUser: AuthUser): F[Unit] =
    coreClient
      .createProject(corePayload, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(CreateFailures.onCoreCreation(newProject.slug, _).raiseError[F, Unit], _.pure[F]))

  private def activateProject(newProject:       NewProject,
                              glCreatedProject: GLCreatedProject,
                              accessToken:      UserAccessToken
  ): F[Unit] =
    wsClient
      .createHook(glCreatedProject.id, accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(
        _.fold(CreateFailures.onActivation(newProject.slug, _).raiseError[F, Unit], checkActivationResult(newProject))
      )

  private def checkActivationResult(newProject: NewProject): HookCreationResult => F[Unit] = {
    case HookCreationResult.Created | HookCreationResult.Existed => ().pure[F]
    case HookCreationResult.NotFound => CreateFailures.activationReturningNotFound(newProject.slug).raiseError[F, Unit]
  }
}
