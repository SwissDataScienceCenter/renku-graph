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

package ch.datascience.commiteventservice.events.categories.commitsync

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.graph.model.projects.Visibility.Public
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait ProjectInfoFinder[Interpretation[_]] {
  def findProjectInfo(
      projectId:        projects.Id,
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[ProjectInfo]
}

private class IOProjectInfoFinder(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with ProjectInfoFinder[IO] {

  import cats.effect._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  def findProjectInfo(projectId: projects.Id, maybeAccessToken: Option[AccessToken]): IO[ProjectInfo] =
    for {
      uri         <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId")
      projectInfo <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield projectInfo

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[ProjectInfo]] = {
    case (Ok, _, response)    => response.as[ProjectInfo]
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit lazy val projectInfoDecoder: EntityDecoder[IO, ProjectInfo] = {
    implicit val hookNameDecoder: Decoder[ProjectInfo] = (cursor: HCursor) =>
      for {
        id         <- cursor.downField("id").as[projects.Id]
        visibility <- cursor.downField("visibility").as[Option[Visibility]] map defaultToPublic
        path       <- cursor.downField("path_with_namespace").as[projects.Path]
      } yield ProjectInfo(id, visibility, path)

    jsonOf[IO, ProjectInfo]
  }

  private def defaultToPublic(maybeVisibility: Option[Visibility]): Visibility =
    maybeVisibility getOrElse Public
}

private object IOProjectInfoFinder {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectInfoFinder[IO]] =
    for {
      gitLabUrl <- GitLabUrl[IO]()
    } yield new IOProjectInfoFinder(gitLabUrl, gitLabThrottler, logger)
}
