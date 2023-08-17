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

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, ProjectUpdates => TGProjectUpdates}
import org.http4s.Response
import org.http4s.Status.{Accepted, BadRequest, Conflict, InternalServerError}
import org.typelevel.log4cats.Logger

private trait ProjectUpdater[F[_]] {
  def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Response[F]]
}

private object ProjectUpdater {
  def apply[F[_]: Async: GitLabClient: MetricsRegistry: Logger]: F[ProjectUpdater[F]] =
    TriplesGeneratorClient[F].map(new ProjectUpdaterImpl[F](BranchProtectionCheck[F], GLProjectUpdater[F], _))
}

private class ProjectUpdaterImpl[F[_]: Async: Logger](branchProtectionCheck: BranchProtectionCheck[F],
                                                      glProjectUpdater: GLProjectUpdater[F],
                                                      tgClient:         TriplesGeneratorClient[F]
) extends ProjectUpdater[F] {

  import branchProtectionCheck.canPushToDefaultBranch

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Response[F]] =
    if ((updates.newDescription orElse updates.newKeywords).isEmpty)
      updateGL(slug, updates, authUser)
        .flatMap(_ => updateTG(slug, updates))
        .merge
    else
      canPushToDefaultBranch(slug, authUser.accessToken)
        .flatMap {
          case false => conflictResponse.pure[F]
          case true  => acceptedResponse.pure[F]
        }

  private def updateGL(slug:     projects.Slug,
                       updates:  ProjectUpdates,
                       authUser: AuthUser
  ): EitherT[F, Response[F], Unit] =
    glProjectUpdater.updateProject(slug, updates, authUser.accessToken).leftMap(badRequest)

  private def badRequest(message: Json): Response[F] =
    Response[F](BadRequest).withEntity(Message.Error.fromJsonUnsafe(message))

  private def updateTG(slug: projects.Slug, updates: ProjectUpdates): EitherT[F, Response[F], Response[F]] =
    EitherT {
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
    }.biSemiflatMap(
      serverError(slug),
      _ => acceptedResponse.pure[F]
    )

  private def serverError(slug: projects.Slug): Throwable => F[Response[F]] =
    Logger[F]
      .error(_)(show"Updating project $slug failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error("Update failed")))

  private lazy val acceptedResponse = Response[F](Accepted).withEntity(Message.Info("Project update accepted"))
  private lazy val conflictResponse = Response[F](Conflict)
    .withEntity(
      Message.Error("Updating project not possible; quite likely the user cannot push to the default branch")
    )
}
