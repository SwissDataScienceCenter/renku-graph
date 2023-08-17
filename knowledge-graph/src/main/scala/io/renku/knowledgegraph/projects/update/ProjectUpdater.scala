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
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.core.client.{RenkuCoreClient, UserInfo, ProjectUpdates => CoreProjectUpdates}
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

  override def updateProject(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser): F[Response[F]] = {
    if ((updates.newDescription orElse updates.newKeywords).isEmpty)
      updateGL(slug, updates, authUser)
        .flatMap(_ => updateTG(slug, updates))
    else
      canPushToDefaultBranch(slug, authUser)
        .flatMap(_ => findCoreProjectUpdates(slug, updates, authUser))
        .flatMap(updates => findCoreUri(updates, authUser).map(updates -> _).map(_ => acceptedResponse))
  }.merge

  private def updateGL(slug:     projects.Slug,
                       updates:  ProjectUpdates,
                       authUser: AuthUser
  ): EitherT[F, Response[F], Unit] = EitherT {
    glProjectUpdater
      .updateProject(slug, updates, authUser.accessToken)
      .map(_.leftMap(badRequest))
      .handleErrorWith(glUpdateError(slug))
  }

  private def badRequest(message: Json): Response[F] =
    Response[F](BadRequest).withEntity(Message.Error.fromJsonUnsafe(message))

  private def glUpdateError(slug: projects.Slug): Throwable => F[Either[Response[F], Unit]] =
    Logger[F]
      .error(_)(show"Updating project $slug in GL failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error("Updating GL failed")).asLeft)

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
        .handleError(_.asLeft)
    }.biSemiflatMap(
      tsUpdateError(slug),
      _ => acceptedResponse.pure[F]
    )

  private def tsUpdateError(slug: projects.Slug): Throwable => F[Response[F]] =
    Logger[F]
      .error(_)(show"Updating project $slug in TS failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error("Updating TS failed")))

  private def canPushToDefaultBranch(slug: projects.Slug, authUser: AuthUser) = EitherT {
    branchProtectionCheck
      .canPushToDefaultBranch(slug, authUser.accessToken)
      .flatMap {
        case false =>
          Response[F](Conflict)
            .withEntity(Message.Error("Updating project not possible; the user cannot push to the default branch"))
            .asLeft[Unit]
            .pure[F]
        case true => ().asRight[Response[F]].pure[F]
      }
      .handleErrorWith(pushCheckError(slug))
  }

  private def pushCheckError(slug: projects.Slug): Throwable => F[Either[Response[F], Unit]] =
    Logger[F]
      .error(_)(show"Check if pushing to git for $slug possible failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error("Finding project repository access failed")).asLeft)

  private def findCoreProjectUpdates(slug: projects.Slug, updates: ProjectUpdates, authUser: AuthUser) = EitherT {
    (findProjectGitUrl(slug, authUser) -> findUserInfo(authUser))
      .parMapN { (maybeProjectGitUrl, maybeUserInfo) =>
        (maybeProjectGitUrl -> maybeUserInfo)
          .mapN(CoreProjectUpdates(_, _, updates.newDescription, updates.newKeywords))
      }
  }

  private def findProjectGitUrl(slug: projects.Slug, authUser: AuthUser) =
    projectGitUrlFinder
      .findGitUrl(slug, authUser.accessToken)
      .flatMap {
        case Some(url) => url.asRight[Response[F]].pure[F]
        case None =>
          Response[F](InternalServerError)
            .withEntity(Message.Error("Cannot find project info"))
            .asLeft[projects.GitHttpUrl]
            .pure[F]
      }
      .handleErrorWith(findingGLUrlError(slug))

  private def findingGLUrlError(slug: projects.Slug): Throwable => F[Either[Response[F], projects.GitHttpUrl]] =
    Logger[F]
      .error(_)(show"Finding git url for $slug failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error("Finding project git url failed")).asLeft)

  private def findUserInfo(authUser: AuthUser) =
    userInfoFinder
      .findUserInfo(authUser.accessToken)
      .flatMap {
        case Some(info) => info.asRight[Response[F]].pure[F]
        case None =>
          Response[F](InternalServerError)
            .withEntity(Message.Error("Cannot find user info"))
            .asLeft[UserInfo]
            .pure[F]
      }
      .handleErrorWith(findingUserInfoError(authUser))

  private def findingUserInfoError(user: AuthUser): Throwable => F[Either[Response[F], UserInfo]] =
    Logger[F]
      .error(_)(show"Finding userInfo for ${user.id} failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error("Finding user info failed")).asLeft)

  private def findCoreUri(updates: CoreProjectUpdates, authUser: AuthUser) = EitherT {
    renkuCoreClient
      .findCoreUri(updates.projectUrl, authUser.accessToken)
      .map(_.toEither)
      .handleError(_.asLeft)
  }.leftSemiflatMap(findingCoreUriError(updates))

  private def findingCoreUriError(updates: CoreProjectUpdates): Throwable => F[Response[F]] = ex =>
    Logger[F]
      .error(ex)(show"Finding core uri for ${updates.projectUrl} failed")
      .as(Response[F](InternalServerError).withEntity(Message.Error.fromExceptionMessage(ex)))

  private lazy val acceptedResponse = Response[F](Accepted).withEntity(Message.Info("Project update accepted"))
}
