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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.commiteventservice.events.categories.common
import io.renku.commiteventservice.events.categories.common.ProjectInfo
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.projects.Visibility.Public
import io.renku.http.client.{AccessToken, GitLabClient}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private trait ProjectInfoFinder[F[_]] {
  def findProjectInfo(projectId: projects.Id)(implicit
      maybeAccessToken:          Option[AccessToken]
  ): F[ProjectInfo]
}

private class ProjectInfoFinderImpl[F[_]: Async: Temporal: Logger](
    gitLabClient: GitLabClient[F]
) extends ProjectInfoFinder[F] {

  import io.circe._
  import io.renku.http.client.RestClientError.UnauthorizedException
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  def findProjectInfo(projectId: projects.Id)(implicit maybeAccessToken: Option[AccessToken]): F[ProjectInfo] =
    gitLabClient.send(GET, uri"projects" / projectId.show, "projects")(mapResponse)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[ProjectInfo]] = {
    case (Ok, _, response)    => response.as[ProjectInfo]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError[F, ProjectInfo]
  }

  private implicit lazy val projectInfoDecoder: EntityDecoder[F, ProjectInfo] = {
    implicit val hookNameDecoder: Decoder[ProjectInfo] = (cursor: HCursor) =>
      for {
        id         <- cursor.downField("id").as[projects.Id]
        visibility <- cursor.downField("visibility").as[Option[Visibility]] map defaultToPublic
        path       <- cursor.downField("path_with_namespace").as[projects.Path]
      } yield common.ProjectInfo(id, visibility, path)

    jsonOf[F, ProjectInfo]
  }

  private def defaultToPublic(maybeVisibility: Option[Visibility]): Visibility =
    maybeVisibility getOrElse Public
}

private object ProjectInfoFinder {
  def apply[F[_]: Async: Temporal: Logger](gitLabClient: GitLabClient[F]): F[ProjectInfoFinder[F]] =
    new ProjectInfoFinderImpl(gitLabClient).pure[F].widen
}
