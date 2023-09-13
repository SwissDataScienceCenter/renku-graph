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

package io.renku.knowledgegraph.projects
package create

import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import com.typesafe.config.Config
import delete.ProjectRemover
import io.renku.core.client.{RenkuCoreClient, NewProject => CorePayload}
import io.renku.http.client.{GitLabClient, UserAccessToken}
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, NewProject => TGNewProject}
import io.renku.webhookservice.api.{HookCreationResult, WebhookServiceClient}
import org.typelevel.log4cats.Logger

private trait ProjectCreator[F[_]] {
  def createProject(newProject: NewProject, authUser: AuthUser): F[Unit]
}

private object ProjectCreator {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: Logger: MetricsRegistry](
      config: Config
  ): F[ProjectCreator[F]] =
    (CorePayloadFinder[F](config),
     RenkuCoreClient[F](config),
     WebhookServiceClient[F](config),
     TriplesGeneratorClient[F](config)
    ).mapN(new ProjectCreatorImpl[F](GLProjectCreator[F], _, _, _, _, ProjectRemover[F]))
}

private class ProjectCreatorImpl[F[_]: MonadThrow: Logger](
    glProjectCreator:  GLProjectCreator[F],
    corePayloadFinder: CorePayloadFinder[F],
    coreClient:        RenkuCoreClient[F],
    wsClient:          WebhookServiceClient[F],
    tgClient:          TriplesGeneratorClient[F],
    glProjectRemover:  ProjectRemover[F]
) extends ProjectCreator[F] {

  override def createProject(newProject: NewProject, authUser: AuthUser): F[Unit] =
    for {
      corePayload <- findCorePayload(newProject, authUser)
      glCreated   <- createProjectInGL(newProject, authUser.accessToken)
      _           <- createProjectInCore(newProject, glCreated, corePayload, authUser)
      _           <- activateProject(newProject, glCreated, authUser.accessToken)
      _           <- createProjectInTG(tgNewProject(newProject, glCreated))
    } yield ()

  private def findCorePayload(newProject: NewProject, authUser: AuthUser): F[CorePayload] =
    corePayloadFinder
      .findCorePayload(newProject, authUser)

  private def createProjectInGL(newProject: NewProject, accessToken: UserAccessToken): F[GLCreatedProject] =
    glProjectCreator
      .createProject(newProject, accessToken)
      .adaptError(CreationFailures.onGLCreation(newProject.slug, _))
      .flatMap(_.fold(_.raiseError[F, GLCreatedProject], _.pure[F]))

  private def createProjectInCore(newProject:       NewProject,
                                  glCreatedProject: GLCreatedProject,
                                  corePayload:      CorePayload,
                                  authUser:         AuthUser
  ): F[Unit] =
    coreClient
      .createProject(corePayload, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatTap {
        case _: Right[_, _] => ().pure[F]
        case _: Left[_, _]  => deleteProjectInGL(glCreatedProject, authUser.accessToken)
      }
      .flatMap(_.fold(CreationFailures.onCoreCreation(newProject.slug, _).raiseError[F, Unit], _.pure[F]))

  private def deleteProjectInGL(glCreatedProject: GLCreatedProject, accessToken: UserAccessToken): F[Unit] =
    glProjectRemover
      .deleteProject(glCreatedProject.id)(accessToken)
      .handleErrorWith(err =>
        Logger[F].warn(show"GL project deletion on Core failure on project creation failed: ${err.getMessage}")
      )

  private def activateProject(newProject:       NewProject,
                              glCreatedProject: GLCreatedProject,
                              accessToken:      UserAccessToken
  ): F[Unit] =
    wsClient
      .createHook(glCreatedProject.id, accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(
        _.fold(CreationFailures.onActivation(newProject.slug, _).raiseError[F, Unit], checkActivationResult(newProject))
      )

  private def checkActivationResult(newProject: NewProject): HookCreationResult => F[Unit] = {
    case HookCreationResult.Created | HookCreationResult.Existed => ().pure[F]
    case HookCreationResult.NotFound => CreationFailures.activationReturningNotFound(newProject).raiseError[F, Unit]
  }

  private def tgNewProject(newProject: NewProject, glCreatedProject: GLCreatedProject): TGNewProject =
    TGNewProject(
      newProject.name,
      newProject.slug,
      newProject.maybeDescription,
      glCreatedProject.dateCreated,
      TGNewProject.Creator(glCreatedProject.creator.name, glCreatedProject.creator.id),
      newProject.keywords,
      newProject.visibility,
      glCreatedProject.maybeImage.toList
    )

  private def createProjectInTG(tgNewProject: TGNewProject): F[Unit] =
    tgClient
      .createProject(tgNewProject)
      .map(_.toEither)
      .handleError(_.asLeft)
      .flatMap(_.fold(CreationFailures.onTGCreation(tgNewProject.slug, _).raiseError[F, Unit], _.pure[F]))
}
